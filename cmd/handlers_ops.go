package cmd

import (
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/mikills/minnow/kb"

	"github.com/labstack/echo/v4"
)

// operationsPollRate is the per-IP sustained rate (requests/sec) allowed on
// the operation polling endpoint. operationsPollBurst is the one-shot burst
// allowance before throttling kicks in.
const (
	operationsPollRate         = 10.0
	operationsPollBurst        = 20.0
	operationsLimiterStripes   = 256
	operationsBucketsPerStripe = 16
)

type ipRateLimiter struct {
	rate    float64
	burst   float64
	stripes []rateLimitStripe
}

type rateLimitStripe struct {
	mu      sync.Mutex
	buckets map[string]*tokenBucket
}

type tokenBucket struct {
	tokens    float64
	updatedNS int64
}

// capabilityIngestSessions tells a client this server can commit a session.
const capabilityIngestSessions = "ingest_sessions"

func newIPRateLimiter(rate, burst float64) *ipRateLimiter {
	return &ipRateLimiter{rate: rate, burst: burst, stripes: make([]rateLimitStripe, operationsLimiterStripes)}
}

func (l *ipRateLimiter) Allow(ip string) bool {
	if ip == "" {
		ip = "unknown"
	}
	stripe := &l.stripes[rateLimitStripeFor(ip)]
	stripe.mu.Lock()
	defer stripe.mu.Unlock()
	if stripe.buckets == nil {
		stripe.buckets = make(map[string]*tokenBucket)
	}
	nowNS := time.Now().UnixNano()
	bucket := stripe.buckets[ip]
	if bucket == nil {
		if len(stripe.buckets) >= operationsBucketsPerStripe {
			evictOldestRateLimitBucket(stripe.buckets)
		}
		bucket = &tokenBucket{tokens: l.burst, updatedNS: nowNS}
		stripe.buckets[ip] = bucket
	}
	elapsed := float64(nowNS-bucket.updatedNS) / float64(time.Second)
	if elapsed > 0 {
		bucket.tokens += elapsed * l.rate
		if bucket.tokens > l.burst {
			bucket.tokens = l.burst
		}
		bucket.updatedNS = nowNS
	}
	if bucket.tokens >= 1 {
		bucket.tokens--
		return true
	}
	return false
}

func evictOldestRateLimitBucket(buckets map[string]*tokenBucket) {
	var oldestKey string
	var oldestNS int64
	for key, bucket := range buckets {
		if oldestKey == "" || bucket.updatedNS < oldestNS {
			oldestKey = key
			oldestNS = bucket.updatedNS
		}
	}
	delete(buckets, oldestKey)
}

func rateLimitStripeFor(value string) int {
	var hash uint32 = 2166136261
	for i := 0; i < len(value); i++ {
		hash ^= uint32(value[i])
		hash *= 16777619
	}
	return int(hash % operationsLimiterStripes)
}

func registerOpsRoutes(e *echo.Echo, deps Dependencies) {
	metrics := deps.AppMetrics

	e.GET("/healthz", func(c echo.Context) error {
		capabilities := []string{}
		if deferredPublishReady(deps) {
			capabilities = append(capabilities, capabilityIngestSessions)
		}
		return c.JSON(http.StatusOK, map[string]any{"status": "ok", "capabilities": capabilities})
	})
	if deps.CacheMetricsHandler != nil {
		e.GET("/metrics/cache", echo.WrapHandler(deps.CacheMetricsHandler))
	}
	e.GET("/metrics/app", func(c echo.Context) error {
		return c.JSON(http.StatusOK, metrics.Snapshot())
	})

	operationsLimiter := newIPRateLimiter(operationsPollRate, operationsPollBurst)
	e.GET("/rag/operations/:id", operationStatusHandler(deps, operationsLimiter))
}

func remoteClientIP(r *http.Request) string {
	if r == nil {
		return "unknown"
	}
	peer := parseRequestIP(r.RemoteAddr)
	if peer == nil {
		return "unknown"
	}
	// The bundled reverse-proxy topology connects over loopback. Private/LAN
	// peers are not implicitly trusted and cannot select their own identity.
	if peer.IsLoopback() {
		if forwarded := parseRequestIP(r.Header.Get("X-Real-IP")); forwarded != nil {
			return forwarded.String()
		}
	}
	return peer.String()
}

func parseRequestIP(value string) net.IP {
	value = strings.TrimSpace(value)
	if len(value) > net.IPv6len*3 {
		return nil
	}
	if host, _, err := net.SplitHostPort(value); err == nil {
		value = host
	}
	return net.ParseIP(strings.Trim(value, "[]"))
}

func operationStatusHandler(deps Dependencies, operationsLimiter *ipRateLimiter) echo.HandlerFunc {
	return func(c echo.Context) error {
		if !operationsLimiter.Allow(remoteClientIP(c.Request())) {
			c.Response().Header().Set("Retry-After", "1")
			return c.JSON(http.StatusTooManyRequests, map[string]any{errorResponseKey: "rate limit exceeded"})
		}
		if deps.GetEvent == nil {
			return c.JSON(
				http.StatusServiceUnavailable,
				map[string]any{errorResponseKey: "event subsystem not configured"},
			)
		}
		id := strings.TrimSpace(c.Param("id"))
		if id == "" {
			return c.JSON(http.StatusBadRequest, map[string]any{errorResponseKey: "id required"})
		}
		ev, err := deps.GetEvent(c.Request().Context(), id)
		if err != nil {
			if errors.Is(err, kb.ErrEventNotFound) {
				return c.JSON(http.StatusNotFound, map[string]any{errorResponseKey: "not found"})
			}
			return c.JSON(http.StatusInternalServerError, map[string]any{errorResponseKey: err.Error()})
		}
		root := operationStatusPayload(c, deps, id, ev)
		return c.JSON(http.StatusOK, root)
	}
}

func operationStatusPayload(c echo.Context, deps Dependencies, id string, ev *kb.KBEvent) map[string]any {
	root := eventStatusPayload(ev)
	root["stages"] = operationStagesPayload(c, deps, id)
	root["terminal"] = operationTerminalPayload(c, deps, id)
	return root
}

func operationTerminalPayload(c echo.Context, deps Dependencies, id string) map[string]any {
	if deps.FindOperationTerminal == nil {
		return nil
	}
	child, err := deps.FindOperationTerminal(c.Request().Context(), id)
	if err != nil || child == nil {
		return nil
	}
	return eventStatusPayload(child)
}

func operationStagesPayload(c echo.Context, deps Dependencies, id string) []map[string]any {
	if deps.OperationStages == nil {
		return nil
	}
	snapshots, err := deps.OperationStages(c.Request().Context(), id)
	if err != nil {
		return nil
	}
	stages := make([]map[string]any, 0, len(snapshots))
	for _, snapshot := range snapshots {
		stages = append(stages, operationStagePayload(snapshot))
	}
	return stages
}

func eventStatusPayload(ev *kb.KBEvent) map[string]any {
	if ev == nil {
		return nil
	}
	out := map[string]any{
		eventIDResponseKey: ev.EventID,
		kbIDContextKey:     ev.KBID,
		"kind":             ev.Kind,
		"status":           ev.Status,
		"attempt":          ev.Attempt,
		"correlation_id":   ev.CorrelationID,
		"causation_id":     ev.CausationID,
		"created_at":       ev.CreatedAt,
		"last_error":       ev.LastError,
	}
	switch ev.Kind {
	case kb.EventKBPublished:
		var payload kb.KBPublishedPayload
		if json.Unmarshal(ev.Payload, &payload) == nil {
			out["document_count"] = payload.DocumentCount
			out["chunk_count"] = payload.ChunkCount
			out["media_ids"] = payload.MediaIDs
			out["file_results"] = payload.FileResults
		}
	case kb.EventMediaUploaded:
		var payload kb.MediaUploadedPayload
		if json.Unmarshal(ev.Payload, &payload) == nil {
			out["media_id"] = payload.MediaID
			out["filename"] = payload.Filename
		}
	case kb.EventWorkerFailed:
		var payload kb.WorkerFailedPayload
		if json.Unmarshal(ev.Payload, &payload) == nil {
			out["stage"] = payload.Stage
			out["will_retry"] = payload.WillRetry
			out["file_results"] = payload.FileResults
		}
	}
	return out
}

func operationStagePayload(snapshot kb.OperationStageSnapshot) map[string]any {
	stage := eventStatusPayload(snapshot.Event)
	if stage == nil {
		stage = map[string]any{}
	}
	if snapshot.Failure != nil {
		stage["failure"] = eventStatusPayload(snapshot.Failure)
	}
	return stage
}
