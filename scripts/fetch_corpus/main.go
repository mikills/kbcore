package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"golang.org/x/net/html"

	"github.com/mikills/minnow/kb"
)

var backgroundContext = context.Background()

type company struct {
	Ticker string
	CIK    string
}

var companies = []company{
	{"AAPL", "0000320193"},
	{"MSFT", "0000789019"},
	{"GOOGL", "0001652044"},
	{"AMZN", "0001018724"},
	{"NVDA", "0001045810"},
	{"META", "0001326801"},
	{"BRKA", "0001067983"},
	{"TSLA", "0001318605"},
	{"JPM", "0000019617"},
	{"WMT", "0000104169"},
	{"XOM", "0000034088"},
	{"JNJ", "0000200406"},
	{"V", "0001403161"},
	{"PG", "0000080424"},
	{"UNH", "0000731766"},
	{"MA", "0001141391"},
	{"HD", "0000354950"},
	{"BAC", "0000070858"},
	{"CVX", "0000093410"},
	{"KO", "0000021344"},
}

type submissionsJSON struct {
	Filings struct {
		Recent struct {
			Form            []string `json:"form"`
			AccessionNumber []string `json:"accessionNumber"`
			PrimaryDocument []string `json:"primaryDocument"`
			FilingDate      []string `json:"filingDate"`
		} `json:"recent"`
	} `json:"filings"`
}

func main() {
	outDir := flag.String("out", "testdata/corpus/10k-filings", "output directory")
	userAgent := flag.String("ua", "minnow-bench contact@example.com", "SEC EDGAR User-Agent (contact info required)")
	chunkSize := flag.Int("chunk", 1000, "approximate chunk size in characters")
	throttleMs := flag.Int("throttle-ms", 500, "sleep between companies to respect SEC rate limits")
	useIndex := flag.Bool("index", false, "read every filer from EDGAR instead of the built-in list")
	target := flag.Int("target", 0, "stop once this many chunks exist; 0 fetches the whole list")
	limit := flag.Int("limit", 0, "stop after this many companies; 0 means no limit")
	resume := flag.Bool("resume", true, "skip a company whose output file already exists")
	flag.Parse()

	if err := os.MkdirAll(*outDir, 0o755); err != nil {
		exit("mkdir: %v", err)
	}

	client := &http.Client{Timeout: 60 * time.Second}
	chunker := kb.TextChunker{ChunkSize: *chunkSize}
	ctx := backgroundContext

	list := companies
	if *useIndex {
		fetched, err := fetchCompanyIndex(ctx, client, *userAgent)
		if err != nil {
			exit("company index: %v", err)
		}
		list = fetched
		fmt.Printf("%d filers from EDGAR\n", len(list))
	}

	totalChunks := countExistingChunks(*outDir)
	if totalChunks > 0 {
		fmt.Printf("%d chunks already on disk\n", totalChunks)
	}
	attempted := 0
	failures := 0
	skipped := 0
	start := time.Now()

	for _, co := range list {
		if *target > 0 && totalChunks >= *target {
			break
		}
		if *limit > 0 && attempted >= *limit {
			break
		}
		outPath := filepath.Join(*outDir, sanitizeTicker(co.Ticker)+".jsonl")
		if *resume {
			if info, err := os.Stat(outPath); err == nil && info.Size() > 0 {
				skipped++
				continue
			}
		}
		previous := 0
		if info, err := os.Stat(outPath); err == nil && info.Size() > 0 {
			previous = countChunks(outPath)
		}
		attempted++
		fmt.Printf("[%s] CIK %s\n", co.Ticker, co.CIK)
		chunks, filingDate, err := fetchOne(ctx, client, *userAgent, co, chunker)
		if err != nil {
			fmt.Fprintf(os.Stderr, "  error: %v\n", err)
			failures++
			time.Sleep(time.Duration(*throttleMs) * time.Millisecond)
			continue
		}
		if err := writeJSONL(outPath, chunks); err != nil {
			exit("write %s: %v", outPath, err)
		}
		totalChunks += len(chunks) - previous
		fmt.Printf("  %d chunks (%d total), 10-K filed %s -> %s\n",
			len(chunks), totalChunks, filingDate, outPath)
		time.Sleep(time.Duration(*throttleMs) * time.Millisecond)
	}

	fmt.Printf("\n%d filings, %d chunks, %d failures, %d already present, elapsed %v\n",
		attempted-failures, totalChunks, failures, skipped, time.Since(start).Round(time.Second))
}

const companyIndexURL = "https://www.sec.gov/files/company_tickers.json"

// fetchCompanyIndex reads every EDGAR filer. The JSON is an object keyed by a
// numeric string in EDGAR's own order, roughly largest first, so sorting by
// that key keeps runs reproducible and takes the biggest filings earliest.
func fetchCompanyIndex(ctx context.Context, client *http.Client, ua string) ([]company, error) {
	body, err := fetch(ctx, client, ua, companyIndexURL)
	if err != nil {
		return nil, err
	}
	var raw map[string]struct {
		CIK    int    `json:"cik_str"`
		Ticker string `json:"ticker"`
	}
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, fmt.Errorf("unmarshal index: %w", err)
	}
	keys := make([]string, 0, len(raw))
	for k := range raw {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		a, _ := strconv.Atoi(keys[i])
		b, _ := strconv.Atoi(keys[j])
		return a < b
	})
	out := make([]company, 0, len(raw))
	seenCIK := make(map[int]struct{}, len(raw))
	seenTicker := make(map[string]struct{}, len(raw))
	for _, k := range keys {
		entry := raw[k]
		ticker := sanitizeTicker(entry.Ticker)
		if ticker == "" {
			continue
		}
		// Share classes list separately under one CIK and one 10-K, so
		// deduping on ticker alone downloads the same filing twice.
		if _, dup := seenCIK[entry.CIK]; dup {
			continue
		}
		if _, dup := seenTicker[ticker]; dup {
			continue
		}
		seenCIK[entry.CIK] = struct{}{}
		seenTicker[ticker] = struct{}{}
		out = append(out, company{Ticker: ticker, CIK: fmt.Sprintf("%010d", entry.CIK)})
	}
	return out, nil
}

// sanitizeTicker keeps a ticker usable as a file name. EDGAR emits forms like
// "BRK-B" and the occasional slash.
func sanitizeTicker(raw string) string {
	var b strings.Builder
	for _, r := range strings.ToUpper(strings.TrimSpace(raw)) {
		switch {
		case r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-', r == '_':
			b.WriteRune(r)
		}
	}
	return b.String()
}

func countExistingChunks(dir string) int {
	matches, err := filepath.Glob(filepath.Join(dir, "*.jsonl"))
	if err != nil {
		return 0
	}
	total := 0
	for _, path := range matches {
		total += countChunks(path)
	}
	return total
}

func countChunks(path string) int {
	f, err := os.Open(path)
	if err != nil {
		return 0
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, 1024*1024), 8*1024*1024)
	total := 0
	for scanner.Scan() {
		if strings.TrimSpace(scanner.Text()) != "" {
			total++
		}
	}
	if scanner.Err() != nil {
		return 0
	}
	return total
}

func fetchOne(
	ctx context.Context,
	client *http.Client,
	ua string,
	co company,
	chunker kb.TextChunker,
) ([]kb.Chunk, string, error) {
	submissionsURL := fmt.Sprintf("https://data.sec.gov/submissions/CIK%s.json", co.CIK)
	body, err := fetch(ctx, client, ua, submissionsURL)
	if err != nil {
		return nil, "", fmt.Errorf("submissions index: %w", err)
	}
	var subs submissionsJSON
	if err := json.Unmarshal(body, &subs); err != nil {
		return nil, "", fmt.Errorf("unmarshal submissions: %w", err)
	}
	r := subs.Filings.Recent
	for i := range r.Form {
		if r.Form[i] != "10-K" {
			continue
		}
		accession := strings.ReplaceAll(r.AccessionNumber[i], "-", "")
		cikTrimmed := strings.TrimLeft(co.CIK, "0")
		docURL := fmt.Sprintf("https://www.sec.gov/Archives/edgar/data/%s/%s/%s",
			cikTrimmed, accession, r.PrimaryDocument[i])
		raw, err := fetch(ctx, client, ua, docURL)
		if err != nil {
			return nil, "", fmt.Errorf("fetch 10-K: %w", err)
		}
		text, err := htmlToText(raw)
		if err != nil {
			return nil, "", fmt.Errorf("html to text: %w", err)
		}
		chunks, err := chunker.Chunk(ctx, co.Ticker, text)
		if err != nil {
			return nil, "", fmt.Errorf("chunk: %w", err)
		}
		return chunks, r.FilingDate[i], nil
	}
	return nil, "", fmt.Errorf("no 10-K in recent filings")
}

func fetch(ctx context.Context, client *http.Client, ua, url string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", ua)
	reply, err := closeableHTTPDo(client, req)
	if err != nil {
		return nil, err
	}
	defer reply.Close()
	if reply.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("HTTP %d: %s", reply.StatusCode, url)
	}
	return io.ReadAll(reply.Body)
}

func htmlToText(data []byte) (string, error) {
	doc, err := html.Parse(strings.NewReader(string(data)))
	if err != nil {
		return "", err
	}
	var sb strings.Builder
	var walk func(*html.Node)
	walk = func(n *html.Node) {
		if n.Type == html.ElementNode {
			switch n.Data {
			case "script", "style", "noscript":
				return
			}
		}
		if n.Type == html.TextNode {
			t := strings.TrimSpace(n.Data)
			if t != "" {
				sb.WriteString(t)
				sb.WriteByte(' ')
			}
		}
		for c := n.FirstChild; c != nil; c = c.NextSibling {
			walk(c)
		}
		if n.Type == html.ElementNode {
			switch n.Data {
			case "p", "div", "br", "tr", "li", "h1", "h2", "h3", "h4", "h5", "h6":
				sb.WriteByte('\n')
			}
		}
	}
	walk(doc)
	return collapseWhitespace(sb.String()), nil
}

func collapseWhitespace(s string) string {
	var out strings.Builder
	out.Grow(len(s))
	lastSpace := false
	for _, r := range s {
		if r == ' ' || r == '\t' {
			if !lastSpace {
				out.WriteByte(' ')
				lastSpace = true
			}
			continue
		}
		if r == '\n' {
			out.WriteByte('\n')
			lastSpace = true
			continue
		}
		if r == '\r' {
			continue
		}
		out.WriteRune(r)
		lastSpace = false
	}
	return out.String()
}

// writeJSONL writes through a temporary file. A partial write left in place
// would be non-empty, so -resume would skip it forever and one malformed line
// makes the whole corpus unreadable.
func writeJSONL(path string, chunks []kb.Chunk) (err error) {
	tmp := path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			f.Close()
			os.Remove(tmp)
		}
	}()
	w := bufio.NewWriter(f)
	for _, c := range chunks {
		rec := map[string]string{"id": c.ChunkID, "text": c.Text}
		data, err := json.Marshal(rec)
		if err != nil {
			return err
		}
		if _, err := w.Write(data); err != nil {
			return err
		}
		if err := w.WriteByte('\n'); err != nil {
			return err
		}
	}
	if err = w.Flush(); err != nil {
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func exit(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
