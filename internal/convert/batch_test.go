package convert

import (
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/ralforion/qvd2parquet/internal/qvd"
	"github.com/ralforion/qvd2parquet/internal/qvdtest"
)

// newTestConverter builds a converter over a one-field synthetic QVD.
func newTestConverter(t *testing.T, f qvdtest.Field, mutate func(*Options)) (*Converter, *qvd.File) {
	t.Helper()
	path := t.TempDir() + "/t.qvd"
	if _, err := qvdtest.Build(path, qvdtest.Table{Name: "T", Fields: []qvdtest.Field{f}}); err != nil {
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
	if mutate != nil {
		mutate(&opts)
	}
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

func TestBatchAppendsNullsAndValues(t *testing.T) {
	f := qvdtest.Field{Name: "I", Type: "INTEGER",
		Symbols: []qvd.Symbol{qvdtest.Int(7), qvdtest.Int(-3)},
		Rows:    []int{0, -1, 1}}
	conv, _ := newTestConverter(t, f, nil)

	b := conv.NewBatch(memory.NewGoAllocator(), 8)
	defer b.Release()
	for _, v := range []Value{{Int: 7}, {Null: true}, {Int: -3}} {
		b.Append(0, v)
		b.EndRow()
	}
	if b.Rows() != 3 {
		t.Fatalf("Rows() = %d, want 3", b.Rows())
	}
	rec := b.NewRecord()
	defer rec.Release()

	col := rec.Column(0).(*array.Int64)
	if rec.NumRows() != 3 {
		t.Fatalf("record has %d rows", rec.NumRows())
	}
	if col.Value(0) != 7 || !col.IsNull(1) || col.Value(2) != -3 {
		t.Errorf("column = %v", col)
	}
	if b.Rows() != 0 {
		t.Errorf("NewRecord should reset the row count, got %d", b.Rows())
	}
}

func TestBatchAppendsDecimalsExactly(t *testing.T) {
	f := qvdtest.Field{Name: "Amount", Type: "MONEY", NDec: 2, Dec: ",",
		Symbols: []qvd.Symbol{qvdtest.DualFloat(1234.56, "1234,56"), qvdtest.DualFloat(-0.07, "-0,07")},
		Rows:    []int{0, 1}}
	conv, _ := newTestConverter(t, f, nil)

	b := conv.NewBatch(memory.NewGoAllocator(), 4)
	defer b.Release()
	for _, symIdx := range []int64{0, 1, -1} {
		var sym qvd.Symbol
		if symIdx >= 0 {
			sym = conv.File.Symbols[0][symIdx]
		} else {
			sym = qvd.Symbol{Kind: qvd.SymbolNull}
		}
		v, err := conv.ConvertAt(0, symIdx, sym)
		if err != nil {
			t.Fatalf("ConvertAt(%d): %v", symIdx, err)
		}
		b.Append(0, v)
		b.EndRow()
	}
	rec := b.NewRecord()
	defer rec.Release()

	col := rec.Column(0).(*array.Decimal128)
	if got := col.Value(0).BigInt().String(); got != "123456" {
		t.Errorf("row 0 scaled = %s, want 123456", got)
	}
	if got := col.Value(1).BigInt().String(); got != "-7" {
		t.Errorf("row 1 scaled = %s, want -7", got)
	}
	if !col.IsNull(2) {
		t.Error("row 2 should be null")
	}
}

func TestBatchAppendsDualTextColumn(t *testing.T) {
	f := qvdtest.Field{Name: "Qty", Type: "INTEGER",
		Symbols: []qvd.Symbol{qvdtest.DualInt(1, "one"), qvdtest.DualInt(2, "two")},
		Rows:    []int{0, 1}}
	conv, qf := newTestConverter(t, f, func(o *Options) { o.Dual = DualColumns })
	if len(conv.Schema.Columns) != 2 {
		t.Fatalf("got %d output columns, want 2", len(conv.Schema.Columns))
	}

	b := conv.NewBatch(memory.NewGoAllocator(), 4)
	defer b.Release()
	for symIdx := int64(0); symIdx < 2; symIdx++ {
		sym := qf.Symbols[0][symIdx]
		for ci := range conv.Schema.Columns {
			v, err := conv.ConvertAt(ci, symIdx, sym)
			if err != nil {
				t.Fatal(err)
			}
			b.Append(ci, v)
		}
		b.EndRow()
	}
	rec := b.NewRecord()
	defer rec.Release()

	nums := rec.Column(0).(*array.Int64)
	texts := rec.Column(1).(*array.String)
	if nums.Value(0) != 1 || nums.Value(1) != 2 {
		t.Errorf("numeric column = %v", nums)
	}
	if texts.Value(0) != "one" || texts.Value(1) != "two" {
		t.Errorf("text column = %v", texts)
	}
}

func TestConvertAtRejectsOutOfRangeDecimalIndex(t *testing.T) {
	f := qvdtest.Field{Name: "Amount", Type: "MONEY", NDec: 2,
		Symbols: []qvd.Symbol{qvdtest.Float(1.5)}, Rows: []int{0}}
	conv, _ := newTestConverter(t, f, nil)
	_, err := conv.ConvertAt(0, 99, qvd.Symbol{Kind: qvd.SymbolFloat, Float: 1.5})
	if err == nil || !strings.Contains(err.Error(), "out of range") {
		t.Fatalf("err = %v, want an out-of-range error", err)
	}
}

func TestSymbolLookupRejectsOutOfRangeID(t *testing.T) {
	f := qvdtest.Field{Name: "I", Type: "INTEGER",
		Symbols: []qvd.Symbol{qvdtest.Int(1)}, Rows: []int{0}}
	_, qf := newTestConverter(t, f, nil)
	_, err := qf.Symbol(0, 5)
	if err == nil {
		t.Fatal("expected an out-of-range symbol error")
	}
	for _, want := range []string{`"I"`, "symbol id 5", "1 entries"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q should mention %q", err, want)
		}
	}
}

func TestStringStrategyFormatsNumericsDeterministically(t *testing.T) {
	f := qvdtest.Field{Name: "V", Type: "ASCII",
		Symbols: []qvd.Symbol{qvdtest.Int(42), qvdtest.Float(1.5), qvdtest.Str("x"),
			qvdtest.DualFloat(2.5, "2,50")},
		Rows: []int{0, 1, 2, 3}}
	conv, qf := newTestConverter(t, f, func(o *Options) { o.Mixed = MixedString })

	want := []string{"42", "1.5", "x", "2,50"} // the display string wins for duals
	for i, w := range want {
		v, err := conv.ConvertAt(0, int64(i), qf.Symbols[0][i])
		if err != nil {
			t.Fatal(err)
		}
		if v.Str != w {
			t.Errorf("symbol %d rendered as %q, want %q", i, v.Str, w)
		}
	}
}

func TestInt64StrategyRejectsFractionalDouble(t *testing.T) {
	f := qvdtest.Field{Name: "I", Type: "INTEGER",
		Symbols: []qvd.Symbol{qvdtest.Int(1)}, Rows: []int{0}}
	conv, _ := newTestConverter(t, f, nil)
	_, err := conv.ConvertAt(0, 0, qvd.Symbol{Kind: qvd.SymbolFloat, Float: 1.5})
	if err == nil || !strings.Contains(err.Error(), "int64") {
		t.Fatalf("err = %v, want an int64 conversion error", err)
	}
}

func TestFormatFloat(t *testing.T) {
	tests := map[float64]string{
		1.5: "1.5", 42: "42", -0.25: "-0.25", 1e20: "1e+20",
	}
	for in, want := range tests {
		if got := formatFloat(in); got != want {
			t.Errorf("formatFloat(%v) = %q, want %q", in, got, want)
		}
	}
}

func utc() *time.Location { return time.UTC }
