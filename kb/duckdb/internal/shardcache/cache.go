package shardcache

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	kb "github.com/mikills/minnow/kb"
)

type BlobStore interface {
	Download(ctx context.Context, key string, dest string) error
}

const cachedShardTouchInterval = time.Second

type Manager struct {
	CacheDir           string
	BlobStore          BlobStore
	EvictCacheIfNeeded func(context.Context, string) error
	ReserveCache       func(context.Context, int64) (func(), error)
}

func (m Manager) EnsureLocalFile(
	ctx context.Context,
	kbID string,
	shard kb.SnapshotShardMetadata,
) (string, bool, error) {
	if strings.TrimSpace(shard.Key) == "" {
		return "", false, fmt.Errorf("shard key is required")
	}
	cacheDir := filepath.Join(m.CacheDir, kbID, "query-shards")
	localPath := filepath.Join(cacheDir, FileName(shard))
	cached, err := m.useCachedIfPresent(ctx, localPath)
	if err != nil || cached {
		return localPathOrEmpty(localPath, cached), cached, err
	}
	releaseReservation, err := m.reserve(ctx, shard.SizeBytes)
	if err != nil {
		return "", false, err
	}
	defer releaseReservation()
	if err := os.MkdirAll(cacheDir, 0o755); err != nil {
		return "", false, err
	}
	tmpPath, err := m.downloadToTemp(ctx, shard, localPath)
	if err != nil {
		return "", false, err
	}
	defer os.Remove(tmpPath)
	if err := os.Rename(tmpPath, localPath); err != nil {
		return "", false, err
	}
	releaseReservation()
	if err := m.evict(ctx, localPath); err != nil {
		if removeErr := os.Remove(localPath); removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) {
			return "", false, fmt.Errorf("%w; remove rejected cache shard: %v", err, removeErr)
		}
		return "", false, err
	}
	return localPath, false, nil
}

func (m Manager) useCachedIfPresent(ctx context.Context, localPath string) (bool, error) {
	if info, err := os.Stat(localPath); err == nil {
		return true, touchCachedShard(ctx, localPath, info.ModTime())
	} else if !errors.Is(err, os.ErrNotExist) {
		return false, err
	}
	return false, nil
}

func touchCachedShard(ctx context.Context, localPath string, lastTouch time.Time) error {
	if time.Since(lastTouch) < cachedShardTouchInterval {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	now := time.Now()
	return os.Chtimes(localPath, now, now)
}

func (m Manager) downloadToTemp(ctx context.Context, shard kb.SnapshotShardMetadata, localPath string) (string, error) {
	tmpPath := fmt.Sprintf("%s.download-%d", localPath, time.Now().UnixNano())
	if err := m.BlobStore.Download(ctx, shard.Key, tmpPath); err != nil {
		return "", err
	}
	if err := VerifyDownloaded(ctx, shard, tmpPath); err != nil {
		if removeErr := os.Remove(tmpPath); removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) {
			return "", fmt.Errorf("%w; remove invalid downloaded shard: %v", err, removeErr)
		}
		return "", err
	}
	return tmpPath, nil
}

func (m Manager) reserve(ctx context.Context, incomingBytes int64) (func(), error) {
	if m.ReserveCache == nil || incomingBytes <= 0 {
		return func() {}, nil
	}
	return m.ReserveCache(ctx, incomingBytes)
}

func (m Manager) evict(ctx context.Context, protectedPath string) error {
	if m.EvictCacheIfNeeded == nil {
		return nil
	}
	return m.EvictCacheIfNeeded(ctx, protectedPath)
}

func localPathOrEmpty(localPath string, ok bool) string {
	if ok {
		return localPath
	}
	return ""
}

func VerifyDownloaded(ctx context.Context, shard kb.SnapshotShardMetadata, tmpPath string) error {
	if err := verifySize(shard, tmpPath); err != nil {
		return err
	}
	return verifyChecksum(ctx, shard, tmpPath)
}

func verifySize(shard kb.SnapshotShardMetadata, tmpPath string) error {
	if shard.SizeBytes <= 0 {
		return nil
	}
	stat, err := os.Stat(tmpPath)
	if err != nil {
		return err
	}
	if stat.Size() != shard.SizeBytes {
		return fmt.Errorf("size mismatch for shard %s", shard.ShardID)
	}
	return nil
}

func verifyChecksum(ctx context.Context, shard kb.SnapshotShardMetadata, tmpPath string) error {
	if shard.SHA256 == "" {
		return nil
	}
	sha, err := kb.FileContentSHA256(ctx, tmpPath)
	if err != nil {
		return err
	}
	if sha != shard.SHA256 {
		return fmt.Errorf("checksum mismatch for shard %s", shard.ShardID)
	}
	return nil
}

func FileName(shard kb.SnapshotShardMetadata) string {
	token := shard.ShardID
	if strings.TrimSpace(token) == "" {
		token = "shard"
	}
	cleanToken := strings.Map(cacheFileNameRune, token)
	if cleanToken == "" {
		cleanToken = "shard"
	}
	digest := sha256.Sum256([]byte(shard.Key + "|" + shard.Version))
	return fmt.Sprintf("%s-%x.duckdb", cleanToken, digest[:8])
}

func cacheFileNameRune(r rune) rune {
	if r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '-' || r == '_' {
		return r
	}
	return '_'
}
