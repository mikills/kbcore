package budget

import "context"

// EmbedParallelism bounds one upsert's requests. AcquireEmbed bounds the
// process, since several upserts run at once and a per-call limit alone lets
// requests to one provider multiply without bound.
func (m *Manager) EmbedParallelism(configured int) int {
	// Each batch in flight holds its inputs and its vectors on the Go heap.
	if m.Pressure() == PressureCritical {
		return 1
	}
	want := DefaultEmbedParallelism
	if configured > 0 {
		want = min(configured, MaxEmbedParallelism)
	}
	if m.Pressure() == PressureHigh {
		want = max(want/2, 1)
	}
	return want
}

// AcquireEmbed reserves one of the process's embedding slots.
func (m *Manager) AcquireEmbed(ctx context.Context) (func(), error) {
	if err := m.embeds.Acquire(ctx, 1); err != nil {
		return nil, err
	}
	return func() { m.embeds.Release(1) }, nil
}
