package duckdb_test

import "testing"

func TestBenchCases(t *testing.T) {
	t.Run("parses a name", func(t *testing.T) {
		cases := []struct {
			spec string
			want benchCase
		}{
			{"1M_dim384", benchCase{"1M_dim384", 1_000_000, 384, false}},
			{"100k_dim768", benchCase{"100k_dim768", 100_000, 768, false}},
			{"5M_dim512", benchCase{"5M_dim512", 5_000_000, 512, false}},
			{"250_dim16", benchCase{"250_dim16", 250, 16, false}},
			{"10k_real_dim768", benchCase{"10k_real_dim768", 10_000, 768, true}},
		}
		for _, tc := range cases {
			got, err := parseBenchCase(tc.spec)
			if err != nil {
				t.Fatalf("parseBenchCase(%q): %v", tc.spec, err)
			}
			if got != tc.want {
				t.Fatalf("parseBenchCase(%q) = %+v, want %+v", tc.spec, got, tc.want)
			}
		}
	})

	t.Run("rejects a bad name", func(t *testing.T) {
		for _, spec := range []string{
			"", "1M", "dim384", "1G_dim384", "1M_dim0", "0_dim384",
			"1M_dim384_extra", "1m_dim384", "-1_dim384",
		} {
			if got, err := parseBenchCase(spec); err == nil {
				t.Fatalf("parseBenchCase(%q) = %+v, want an error", spec, got)
			}
		}
	})

	t.Run("empty keeps the default suite", func(t *testing.T) {
		got, err := benchCases("  ")
		if err != nil {
			t.Fatal(err)
		}
		if len(got) != len(defaultBenchCases()) {
			t.Fatalf("got %d cases, want the default suite", len(got))
		}
	})

	t.Run("parses a list", func(t *testing.T) {
		got, err := benchCases(" 2M_dim512 , 5M_dim512 ")
		if err != nil {
			t.Fatal(err)
		}
		want := []benchCase{
			{"2M_dim512", 2_000_000, 512, false},
			{"5M_dim512", 5_000_000, 512, false},
		}
		if len(got) != len(want) {
			t.Fatalf("got %d cases, want %d", len(got), len(want))
		}
		for i := range want {
			if got[i] != want[i] {
				t.Fatalf("case %d = %+v, want %+v", i, got[i], want[i])
			}
		}
	})

	t.Run("rejects a bad entry in a list", func(t *testing.T) {
		if _, err := benchCases("2M_dim512,nonsense"); err == nil {
			t.Fatal("want an error for a bad entry")
		}
	})
}
