package kb

import (
	"testing"
)

// buildBenchShards creates a slice of SnapshotShardMetadata with centroids
// of the given dimension for use in ranking benchmarks.
func buildBenchShards(n, dim int) []SnapshotShardMetadata {
	shards := make([]SnapshotShardMetadata, n)
	for i := range shards {
		c := make([]float32, dim)
		for j := range c {
			c[j] = float32(i*dim+j) * 0.01
		}
		shards[i] = SnapshotShardMetadata{
			ShardID:    "shard-%04d",
			Key:        "key-%04d",
			VectorRows: int64(100 + i),
			Centroid:   c,
		}
	}
	return shards
}

// buildBenchQueryVec returns a query vector of the given dimension.
func buildBenchQueryVec(dim int) []float32 {
	v := make([]float32, dim)
	for i := range v {
		v[i] = float32(i) * 0.001
	}
	return v
}

// BenchmarkRankShardsForQuery measures the cost of ranking shards by
// centroid proximity. The current implementation recomputes shardRankScore
// inside the sort comparator O(n log n) times.
func BenchmarkRankShardsForQuery(b *testing.B) {
	const dim = 64
	for _, n := range []int{4, 16, 64, 256} {
		shards := buildBenchShards(n, dim)
		qVec := buildBenchQueryVec(dim)
		b.Run("shards=%d", func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = rankShardsForQuery(shards, qVec)
			}
		})
	}
}

// BenchmarkShardRankScore measures shardRankScore directly, which currently
// calls math.Sqrt even though ordering only needs squared distance.
func BenchmarkShardRankScore(b *testing.B) {
	const dim = 64
	shard := buildBenchShards(1, dim)[0]
	qVec := buildBenchQueryVec(dim)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = shardRankScore(shard, qVec)
	}
}

// BenchmarkMergeShardTopKResults measures result merging from multiple
// shards. The current implementation allocates the flattened slice with
// no capacity hint, causing incremental growth.
func BenchmarkMergeShardTopKResults(b *testing.B) {
	for _, shardCount := range []int{2, 4, 8} {
		for _, resultsPerShard := range []int{10, 100} {
			shardResults := make([][]QueryResult, shardCount)
			for i := range shardResults {
				rows := make([]QueryResult, resultsPerShard)
				for j := range rows {
					rows[j] = QueryResult{
						ID:       "doc-%04d-%04d",
						Distance: float64(i*resultsPerShard+j) * 0.001,
					}
				}
				shardResults[i] = rows
			}
			k := shardCount * resultsPerShard / 2
			b.Run("shards=%d_results=%d", func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					_ = mergeShardTopKResults(shardResults, k)
				}
			})
		}
	}
}
