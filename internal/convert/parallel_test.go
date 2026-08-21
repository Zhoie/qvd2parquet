package convert

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/ralforion/qvd2parquet/internal/qvd"
	"github.com/ralforion/qvd2parquet/internal/qvdtest"
)

// collectSink records every record it receives, keeping them alive so the test
// can inspect the full result set.
type collectSink struct {
	mu      sync.Mutex
	rows    int64
	records []arrow.Record
	failAt  int64 // fail once this many rows have been accepted; 0 disables
}

func (s *collectSink) Write(rec arrow.Record) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failAt > 0 && s.rows >= s.failAt {
		return fmt.Errorf("%w: injected sink failure", errSink)
	}
	rec.Retain()
	s.records = append(s.records, rec)
	s.rows += rec.NumRows()
	return nil
}

func (s *collectSink) release() {
	for _, r := range s.records {
		r.Release()
	}
}

// errSink stands in for a writer-side failure in tests.
var errSink = errors.New("sink error")

// parallelFixture builds a table whose Id column encodes the source row number,
// so the test can prove every row arrived exactly once despite unordered
// chunk delivery.
func parallelFixture(t *testing.T, rows int) (*Converter, *qvd.File) {
	t.Helper()
	syms := make([]qvd.Symbol, rows)
	idx := make([]int, rows)
	for i := 0; i < rows; i++ {
		syms[i] = qvdtest.Int(int64(i))
		idx[i] = i
	}
	path := filepath.Join(t.TempDir(), "p.qvd")
	if _, err := qvdtest.Build(path, qvdtest.Table{Name: "P", Fields: []qvdtest.Field{
		{Name: "Id", Type: "INTEGER", Symbols: syms, Rows: idx},
	}}); err != nil {
		t.Fatal(err)
	}
	qf, err := qvd.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { qf.Close() })
	if err := qf.ReadSymbols(qvd.UnknownSymbolError); err != nil {
		t.Fatal(err)
	}
	opts := DefaultOptions()
	opts.Location = utc()
	opts.BatchRows = 97 // deliberately not a divisor of the row count
	opts.Workers = 4
	opts.ProgressEvery = 0
	if err := opts.Validate(); err != nil {
		t.Fatal(err)
	}
	rs, err := ResolveSchema(qf, &opts, nil)
	if err != nil {
		t.Fatal(err)
	}
	conv, err := NewConverter(qf, rs, &opts)
	if err != nil {
		t.Fatal(err)
	}
	return conv, qf
}

func TestParallelDecodeCoversEveryRowExactlyOnce(t *testing.T) {
	const rows = 2000
	conv, _ := parallelFixture(t, rows)
	sink := &collectSink{}
	defer sink.release()

	metrics, err := conv.Run(context.Background(), sink, nil)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if sink.rows != rows {
		t.Errorf("sink received %d rows, want %d", sink.rows, rows)
	}
	if metrics.Rows != rows {
		t.Errorf("metrics counted %d rows, want %d", metrics.Rows, rows)
	}

	// Every Id must appear exactly once, in any order.
	seen := make([]int, rows)
	for _, rec := range sink.records {
		col := rec.Column(0)
		for i := 0; i < int(rec.NumRows()); i++ {
			v, err := arrowValue(col, i, StrategyInt64)
			if err != nil {
				t.Fatal(err)
			}
			if v.Int < 0 || v.Int >= rows {
				t.Fatalf("decoded Id %d out of range", v.Int)
			}
			seen[v.Int]++
		}
	}
	for id, n := range seen {
		if n != 1 {
			t.Fatalf("Id %d appeared %d times, want exactly once", id, n)
		}
	}
}

func TestParallelMetricsMergeAcrossChunks(t *testing.T) {
	const rows = 1500
	conv, _ := parallelFixture(t, rows)
	sink := &collectSink{}
	defer sink.release()

	metrics, err := conv.Run(context.Background(), sink, nil)
	if err != nil {
		t.Fatal(err)
	}
	// Ids are 0..rows-1, so the sum is known exactly.
	want := fmt.Sprint(rows * (rows - 1) / 2)
	got := metrics.Columns[0].Stats()
	if got.Sum != want {
		t.Errorf("merged sum = %s, want %s", got.Sum, want)
	}
	if got.Min != "0" || got.Max != fmt.Sprint(rows-1) {
		t.Errorf("merged range = [%s,%s], want [0,%d]", got.Min, got.Max, rows-1)
	}
	if got.NonNulls != rows || got.Nulls != 0 {
		t.Errorf("merged counts = %d non-null, %d null", got.NonNulls, got.Nulls)
	}
}

func TestParallelWorkerCountsProduceIdenticalMetrics(t *testing.T) {
	const rows = 1200
	var reference ColumnStats
	for _, workers := range []int{1, 2, 8} {
		conv, _ := parallelFixture(t, rows)
		conv.Options.Workers = workers
		conv.Options.Quality = QualityFull
		sink := &collectSink{}
		metrics, err := conv.Run(context.Background(), sink, nil)
		sink.release()
		if err != nil {
			t.Fatalf("workers=%d: %v", workers, err)
		}
		got := metrics.Columns[0].Stats()
		if workers == 1 {
			reference = got
			continue
		}
		if got.Hash != reference.Hash || got.Sum != reference.Sum {
			t.Errorf("workers=%d produced hash %s sum %s, want %s / %s",
				workers, got.Hash, got.Sum, reference.Hash, reference.Sum)
		}
	}
}

func TestParallelSinkErrorAbortsRun(t *testing.T) {
	conv, _ := parallelFixture(t, 5000)
	sink := &collectSink{failAt: 100}
	defer sink.release()

	_, err := conv.Run(context.Background(), sink, nil)
	if err == nil {
		t.Fatal("a sink failure should abort the run")
	}
	if !errors.Is(err, errSink) {
		t.Errorf("err = %v, want the injected sink error", err)
	}
}

func TestParallelDecodeErrorAbortsRun(t *testing.T) {
	conv, qf := parallelFixture(t, 3000)
	// Truncate the symbol table so record decoding hits an out-of-range id.
	qf.Symbols[0] = qf.Symbols[0][:10]

	_, err := conv.Run(context.Background(), &collectSink{}, nil)
	if err == nil {
		t.Fatal("an out-of-range symbol id should abort the run")
	}
	if !errors.Is(err, ErrInput) {
		t.Errorf("err = %v, want ErrInput", err)
	}
}

func TestParallelWorkerBuildersAreIndependent(t *testing.T) {
	// Two workers running the same converter must not share builder state.
	// Running with more workers than chunks would serialize, so use many rows.
	const rows = 5000
	conv, _ := parallelFixture(t, rows)
	conv.Options.Workers = 8
	conv.Options.BatchRows = 64

	sink := &collectSink{}
	defer sink.release()
	if _, err := conv.Run(context.Background(), sink, nil); err != nil {
		t.Fatal(err)
	}
	// If builders were shared, records would carry stray or duplicated rows.
	var total int64
	for _, rec := range sink.records {
		if rec.NumRows() == 0 || rec.NumRows() > 64 {
			t.Errorf("record has %d rows, want 1..64", rec.NumRows())
		}
		total += rec.NumRows()
	}
	if total != rows {
		t.Errorf("records hold %d rows in total, want %d", total, rows)
	}
}

func TestRunRemovesTempOutputOnWorkerError(t *testing.T) {
	// A record referencing a symbol beyond the table aborts mid-write.
	rows := 4000
	syms := make([]qvd.Symbol, rows)
	idx := make([]int, rows)
	for i := range syms {
		syms[i] = qvdtest.Int(int64(i))
		idx[i] = i
	}
	dir := t.TempDir()
	in := filepath.Join(dir, "in.qvd")
	if _, err := qvdtest.Build(in, qvdtest.Table{Name: "P", Fields: []qvdtest.Field{
		{Name: "Id", Type: "INTEGER", Symbols: syms, Rows: idx},
	}}); err != nil {
		t.Fatal(err)
	}
	// Corrupt the header so it claims fewer symbols than the records reference.
	raw, err := os.ReadFile(in)
	if err != nil {
		t.Fatal(err)
	}
	corrupted := replaceOnce(raw, []byte("<NoOfSymbols>4000</NoOfSymbols>"), []byte("<NoOfSymbols>0010</NoOfSymbols>"))
	if corrupted == nil {
		t.Skip("fixture header did not contain the expected NoOfSymbols element")
	}
	if err := os.WriteFile(in, corrupted, 0o644); err != nil {
		t.Fatal(err)
	}

	out := filepath.Join(dir, "out.parquet")
	opts := DefaultOptions()
	opts.Location = utc()
	opts.ProgressEvery = 0
	opts.Compression = "snappy"
	opts.BatchRows = 64
	if _, _, err := Run(context.Background(), in, out, &opts, nil); err == nil {
		t.Fatal("expected the conversion to fail")
	}
	if _, err := os.Stat(out); err == nil {
		t.Error("the final output must not exist after a failure")
	}
	entries, _ := os.ReadDir(dir)
	for _, e := range entries {
		if filepath.Ext(e.Name()) != ".qvd" && e.Name() != "out.parquet" {
			t.Errorf("leftover file %q after a failed conversion", e.Name())
		}
	}
}

// replaceOnce replaces the first occurrence of old with repl, keeping the
// length the same so header offsets stay valid. It returns nil if old is absent.
func replaceOnce(b, old, repl []byte) []byte {
	if len(old) != len(repl) {
		panic("replaceOnce needs equal lengths")
	}
	i := bytes.Index(b, old)
	if i < 0 {
		return nil
	}
	out := bytes.Clone(b)
	copy(out[i:], repl)
	return out
}
