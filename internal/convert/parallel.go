package convert

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"runtime"
	"sync"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/ralforion/qvd2parquet/internal/qvd"
)

// ErrInput marks an input read/decode failure (CLI exit code 4).
var ErrInput = errors.New("input error")

// DecodeChunk is one contiguous range of records assigned to a worker.
type DecodeChunk struct {
	Index      int64
	StartRow   int64
	RowCount   int
	ByteOffset int64
}

// DecodeResult is a finished chunk handed to the writer goroutine.
type DecodeResult struct {
	Chunk   DecodeChunk
	Record  arrow.Record
	Metrics *Metrics
}

// RecordSink consumes finished chunks. Implementations are called from a
// single goroutine and need not be safe for concurrent use.
type RecordSink interface {
	Write(arrow.Record) error
}

// Chunks splits the record area into contiguous work items of at most
// batchRows rows each.
func Chunks(rows int64, batchRows int, recordSize int, recordStart int64) []DecodeChunk {
	if rows <= 0 || batchRows <= 0 {
		return nil
	}
	n := (rows + int64(batchRows) - 1) / int64(batchRows)
	out := make([]DecodeChunk, 0, n)
	for i := int64(0); i < n; i++ {
		start := i * int64(batchRows)
		count := int64(batchRows)
		if start+count > rows {
			count = rows - start
		}
		out = append(out, DecodeChunk{
			Index:      i,
			StartRow:   start,
			RowCount:   int(count),
			ByteOffset: recordStart + start*int64(recordSize),
		})
	}
	return out
}

// WorkerCount resolves the --workers option.
func WorkerCount(requested int, chunks int) int {
	n := requested
	if n <= 0 {
		n = runtime.NumCPU()
	}
	if chunks > 0 && n > chunks {
		n = chunks
	}
	if n < 1 {
		n = 1
	}
	return n
}

// ProgressFunc is called with the cumulative number of rows written.
type ProgressFunc func(rows int64)

// Run decodes every record in parallel and streams the resulting Arrow records
// into sink. Chunks are written as workers finish, so the QVD's physical row
// order is not preserved.
func (c *Converter) Run(ctx context.Context, sink RecordSink, progress ProgressFunc) (*Metrics, error) {
	f := c.File
	chunks := Chunks(f.NoOfRecords, c.Options.BatchRows, f.RecordByteSize, f.RecordStart)
	total := NewMetrics(c.Schema)

	if len(chunks) == 0 {
		return total, nil
	}

	workers := WorkerCount(c.Options.Workers, len(chunks))
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	work := make(chan DecodeChunk)
	results := make(chan DecodeResult, workers)

	var wg sync.WaitGroup
	var errOnce sync.Once
	var firstErr error
	fail := func(err error) {
		errOnce.Do(func() {
			firstErr = err
			cancel()
		})
	}

	// Feeder.
	go func() {
		defer close(work)
		for _, ch := range chunks {
			select {
			case work <- ch:
			case <-ctx.Done():
				return
			}
		}
	}()

	// Workers.
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			w := c.newWorker()
			defer w.release()
			for ch := range work {
				if ctx.Err() != nil {
					return
				}
				res, err := w.decodeChunk(ch)
				if err != nil {
					fail(err)
					return
				}
				select {
				case results <- res:
				case <-ctx.Done():
					res.Record.Release()
					return
				}
			}
		}()
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	// Single writer goroutine: this goroutine. The Parquet writer is not safe
	// for concurrent use, so only decoding runs in parallel.
	var written, lastReported int64
	nextProgress := c.Options.ProgressEvery
	for res := range results {
		if firstErr == nil {
			if err := sink.Write(res.Record); err != nil {
				fail(err)
			} else {
				total.Merge(res.Metrics)
				written += res.Record.NumRows()
				if progress != nil && c.Options.ProgressEvery > 0 && written >= nextProgress {
					progress(written)
					lastReported = written
					for written >= nextProgress {
						nextProgress += c.Options.ProgressEvery
					}
				}
			}
		}
		res.Record.Release()
	}

	if firstErr != nil {
		return nil, firstErr
	}
	if err := ctx.Err(); err != nil && !errors.Is(err, context.Canceled) {
		return nil, err
	}
	if written != f.NoOfRecords {
		return nil, fmt.Errorf("%w: wrote %d rows but the header declares %d", ErrInput, written, f.NoOfRecords)
	}
	if progress != nil && c.Options.ProgressEvery > 0 && written != lastReported {
		progress(written)
	}
	return total, nil
}

// worker holds the per-goroutine state: its own Arrow builders, record buffer
// and scratch slices. Nothing here is shared between goroutines.
type worker struct {
	c      *Converter
	file   *os.File
	batch  *Batch
	raw    []byte
	symIdx []int64
	hash   bool
	mem    memory.Allocator
}

func (c *Converter) newWorker() *worker {
	mem := memory.NewGoAllocator()
	return &worker{
		c:      c,
		file:   c.File.FileHandle(),
		batch:  c.NewBatch(mem, c.Options.BatchRows),
		raw:    make([]byte, c.Options.BatchRows*c.File.RecordByteSize),
		symIdx: make([]int64, len(c.File.Columns)),
		hash:   c.Options.Quality == QualityFull,
		mem:    mem,
	}
}

func (w *worker) release() { w.batch.Release() }

// decodeChunk reads one contiguous byte range and converts it into an Arrow
// record plus chunk-local quality metrics.
func (w *worker) decodeChunk(ch DecodeChunk) (DecodeResult, error) {
	f := w.c.File
	size := ch.RowCount * f.RecordByteSize
	buf := w.raw[:size]
	if _, err := w.file.ReadAt(buf, ch.ByteOffset); err != nil && !errors.Is(err, io.EOF) {
		return DecodeResult{}, fmt.Errorf("%w: read rows %d..%d at offset %d: %v",
			ErrInput, ch.StartRow, ch.StartRow+int64(ch.RowCount), ch.ByteOffset, err)
	}

	metrics := NewMetrics(w.c.Schema)
	cols := f.Columns
	outCols := w.c.Schema.Columns

	for r := 0; r < ch.RowCount; r++ {
		rec := buf[r*f.RecordByteSize : (r+1)*f.RecordByteSize]
		if err := qvd.DecodeRecord(rec, cols, w.symIdx); err != nil {
			return DecodeResult{}, fmt.Errorf("%w: row %d: %v", ErrInput, ch.StartRow+int64(r), err)
		}
		for ci := range outCols {
			src := outCols[ci].SourceIndex
			sym, err := f.Symbol(src, w.symIdx[src])
			if err != nil {
				return DecodeResult{}, fmt.Errorf("%w: row %d: %v", ErrInput, ch.StartRow+int64(r), err)
			}
			v, err := w.c.ConvertAt(ci, w.symIdx[src], sym)
			if err != nil {
				return DecodeResult{}, fmt.Errorf("%w: row %d: %v", ErrInput, ch.StartRow+int64(r), err)
			}
			w.batch.Append(ci, v)
			metrics.Columns[ci].Observe(v, w.hash)
		}
		w.batch.EndRow()
	}
	metrics.Rows = int64(ch.RowCount)

	return DecodeResult{Chunk: ch, Record: w.batch.NewRecord(), Metrics: metrics}, nil
}
