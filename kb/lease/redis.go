package lease

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

const DefaultRedisPrefix = "minnow:lease:"

type RedisManager struct {
	Client redis.UniversalClient
	Prefix string

	clockMu sync.RWMutex
	clock   Clock
}

func NewRedisManager(client redis.UniversalClient, prefix string) (*RedisManager, error) {
	if client == nil {
		return nil, fmt.Errorf("redis client is required")
	}
	if strings.TrimSpace(prefix) == "" {
		prefix = DefaultRedisPrefix
	}
	return &RedisManager{Client: client, Prefix: prefix, clock: RealClock}, nil
}

func (m *RedisManager) SetClock(c Clock) {
	m.clockMu.Lock()
	defer m.clockMu.Unlock()
	if c == nil {
		m.clock = RealClock
		return
	}
	m.clock = c
}

func (m *RedisManager) now() time.Time {
	m.clockMu.RLock()
	defer m.clockMu.RUnlock()
	return m.clock.Now()
}

func (m *RedisManager) Acquire(ctx context.Context, kbID string, ttl time.Duration) (*Lease, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if strings.TrimSpace(kbID) == "" {
		return nil, fmt.Errorf("kbID cannot be empty")
	}
	if ttl <= 0 {
		ttl = DefaultTTL
	}
	token, err := randomToken()
	if err != nil {
		return nil, err
	}
	now := m.now()
	ok, err := m.Client.SetNX(ctx, m.key(kbID), token, ttl).Result()
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, ErrConflict
	}
	return &Lease{KBID: kbID, Token: token, ExpiresAt: now.Add(ttl)}, nil
}

func (m *RedisManager) Peek(ctx context.Context, kbID string) (*Lease, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	token, err := m.Client.Get(ctx, m.key(kbID)).Result()
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	ttl, err := m.Client.PTTL(ctx, m.key(kbID)).Result()
	if err != nil {
		return nil, err
	}
	return &Lease{KBID: kbID, Token: token, ExpiresAt: m.now().Add(ttl)}, nil
}

func (m *RedisManager) Renew(ctx context.Context, lease *Lease, ttl time.Duration) (*Lease, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if lease == nil || strings.TrimSpace(lease.KBID) == "" || strings.TrimSpace(lease.Token) == "" {
		return nil, fmt.Errorf("valid lease is required")
	}
	if ttl <= 0 {
		ttl = DefaultTTL
	}
	now := m.now()
	res, err := renewScript.Run(ctx, m.Client, []string{m.key(lease.KBID)}, lease.Token, ttl.Milliseconds()).Int()
	if err != nil {
		return nil, err
	}
	if res != 1 {
		return nil, ErrConflict
	}
	return &Lease{KBID: lease.KBID, Token: lease.Token, ExpiresAt: now.Add(ttl)}, nil
}

var redisReleaseRootContext = context.Background()

func (m *RedisManager) Release(ctx context.Context, lease *Lease) error {
	if lease == nil || strings.TrimSpace(lease.KBID) == "" || strings.TrimSpace(lease.Token) == "" {
		return nil
	}
	if ctx == nil {
		ctx = redisReleaseRootContext
	}
	releaseCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	_, err := releaseScript.Run(releaseCtx, m.Client, []string{m.key(lease.KBID)}, lease.Token).Int()
	return err
}

func (m *RedisManager) key(kbID string) string { return m.Prefix + kbID }

func randomToken() (string, error) {
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		return "", fmt.Errorf("generate random token: %w", err)
	}
	return hex.EncodeToString(buf), nil
}

var renewScript = redis.NewScript(`
local current = redis.call('GET', KEYS[1])
if current == ARGV[1] then
  redis.call('PEXPIRE', KEYS[1], ARGV[2])
  return 1
end
return 0
`)

var releaseScript = redis.NewScript(`
local current = redis.call('GET', KEYS[1])
if current == ARGV[1] then
  redis.call('DEL', KEYS[1])
  return 1
end
return 0
`)
