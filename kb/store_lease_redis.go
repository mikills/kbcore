package kb

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

const defaultRedisLeasePrefix = "kbcore:lease:"

// RedisWriteLeaseManager coordinates per-KB write leases via Redis.
//
// It is intended as the distributed WriteLeaseManager implementation for
// multi-pod deployments (for example, Kubernetes), where an in-process mutex
// cannot protect writes across instances.
//
// System fit:
//   - KB write paths acquire a lease before upload/update work.
//   - The lease ensures only one writer per KB ID is active cluster-wide.
//   - Blob version checks (UploadIfMatch) remain the final consistency guard.
//
// This combines lease-based coordination (to reduce concurrent work) with
// optimistic CAS on blob upload (to prevent lost updates).
//
// Redis semantics:
//   - Acquire uses SET NX PX for atomic lock-with-TTL.
//   - Renew uses a token-checked Lua script (GET + PEXPIRE).
//   - Release uses a token-checked Lua script (GET + DEL).
//
// Token checks are required so one writer cannot accidentally renew/release
// another writer's lease.
type RedisWriteLeaseManager struct {
	Client redis.UniversalClient
	Prefix string
}

// NewRedisWriteLeaseManager creates a Redis-backed lease manager.
//
// Prefix namespaces lease keys, so multiple environments/services can share
// one Redis cluster safely. If prefix is empty, a default namespace is used.
func NewRedisWriteLeaseManager(client redis.UniversalClient, prefix string) (*RedisWriteLeaseManager, error) {
	if client == nil {
		return nil, fmt.Errorf("redis client is required")
	}
	if strings.TrimSpace(prefix) == "" {
		prefix = defaultRedisLeasePrefix
	}
	return &RedisWriteLeaseManager{Client: client, Prefix: prefix}, nil
}

// Acquire attempts to acquire a lease for kbID for the given ttl.
//
// The lease key is namespaced as <prefix><kbID>. On conflict, it returns
// ErrWriteLeaseConflict.
func (m *RedisWriteLeaseManager) Acquire(ctx context.Context, kbID string, ttl time.Duration) (*WriteLease, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if strings.TrimSpace(kbID) == "" {
		return nil, fmt.Errorf("kbID cannot be empty")
	}
	if ttl <= 0 {
		ttl = defaultWriteLeaseTTL
	}

	token, err := randomToken()
	if err != nil {
		return nil, err
	}

	now := time.Now().UTC()
	ok, err := m.Client.SetNX(ctx, m.key(kbID), token, ttl).Result()
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, ErrWriteLeaseConflict
	}

	return &WriteLease{KBID: kbID, Token: token, ExpiresAt: now.Add(ttl)}, nil
}

// Renew extends an existing lease when the token still owns the key.
//
// If the key is missing, expired, or owned by another token, it returns
// ErrWriteLeaseConflict.
func (m *RedisWriteLeaseManager) Renew(ctx context.Context, lease *WriteLease, ttl time.Duration) (*WriteLease, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if lease == nil || strings.TrimSpace(lease.KBID) == "" || strings.TrimSpace(lease.Token) == "" {
		return nil, fmt.Errorf("valid lease is required")
	}
	if ttl <= 0 {
		ttl = defaultWriteLeaseTTL
	}

	now := time.Now().UTC()
	res, err := renewLeaseScript.Run(ctx, m.Client, []string{m.key(lease.KBID)}, lease.Token, ttl.Milliseconds()).Int()
	if err != nil {
		return nil, err
	}
	if res != 1 {
		return nil, ErrWriteLeaseConflict
	}

	return &WriteLease{KBID: lease.KBID, Token: lease.Token, ExpiresAt: now.Add(ttl)}, nil
}

// Release deletes an existing lease only if the token still owns the key.
//
// Release is idempotent for missing/invalid leases and does not return conflict
// if another writer owns the key; ownership is enforced by token matching.
//
// Release always attempts the Redis call regardless of the caller's context
// state. A cancelled or deadline-exceeded context must not prevent the lock
// from being freed; failing to release would block all subsequent writers until
// the TTL expires.
func (m *RedisWriteLeaseManager) Release(_ context.Context, lease *WriteLease) error {
	if lease == nil || strings.TrimSpace(lease.KBID) == "" || strings.TrimSpace(lease.Token) == "" {
		return nil
	}

	releaseCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := releaseLeaseScript.Run(releaseCtx, m.Client, []string{m.key(lease.KBID)}, lease.Token).Int()
	return err
}

func (m *RedisWriteLeaseManager) key(kbID string) string {
	return m.Prefix + kbID
}

func randomToken() (string, error) {
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		return "", fmt.Errorf("generate random token: %w", err)
	}
	return hex.EncodeToString(buf), nil
}

var renewLeaseScript = redis.NewScript(`
local current = redis.call('GET', KEYS[1])
if current == ARGV[1] then
  redis.call('PEXPIRE', KEYS[1], ARGV[2])
  return 1
end
return 0
`)

var releaseLeaseScript = redis.NewScript(`
local current = redis.call('GET', KEYS[1])
if current == ARGV[1] then
  redis.call('DEL', KEYS[1])
  return 1
end
return 0
`)
