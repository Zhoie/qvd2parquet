package qvd

import (
	"math/rand"
	"strings"
	"testing"
)

// referenceBits reimplements the Java reference reader's per-bit accumulation:
// bit n of the record lives in raw[n/8] at position n%8 counting from the least
// significant bit, and bits gain significance as n increases.
func referenceBits(raw []byte, bitOffset, bitWidth int) uint64 {
	var v uint64
	for i := 0; i < bitWidth; i++ {
		p := bitOffset + i
		if raw[p/8]&(1<<uint(p%8)) != 0 {
			v |= 1 << uint(i)
		}
	}
	return v
}

func TestReadBitsLEMatchesReference(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	raw := make([]byte, 24)
	for i := range raw {
		raw[i] = byte(rng.Intn(256))
	}
	for offset := 0; offset <= 64; offset++ {
		for width := 1; width <= 64 && offset+width <= len(raw)*8; width++ {
			got := readBitsLE(raw, offset, width)
			want := referenceBits(raw, offset, width)
			if got != want {
				t.Fatalf("readBitsLE(offset=%d width=%d) = %#x, want %#x", offset, width, got, want)
			}
		}
	}
}

func TestReadBitsLEEdgeCases(t *testing.T) {
	all := []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}
	if got := readBitsLE(all, 0, 64); got != ^uint64(0) {
		t.Errorf("64-bit read = %#x, want all ones", got)
	}
	if got := readBitsLE(all, 1, 64); got != ^uint64(0) {
		t.Errorf("unaligned 64-bit read = %#x, want all ones", got)
	}
	if got := readBitsLE(all, 3, 0); got != 0 {
		t.Errorf("zero-width read = %d, want 0", got)
	}
	// 0x01 0x02 => bits: byte0=00000001, byte1=00000010 (LSB first).
	raw := []byte{0x01, 0x02}
	if got := readBitsLE(raw, 0, 16); got != 0x0201 {
		t.Errorf("16-bit read = %#x, want 0x0201", got)
	}
	if got := readBitsLE(raw, 8, 8); got != 0x02 {
		t.Errorf("second byte = %#x, want 0x02", got)
	}
}

func TestDecodeRecord(t *testing.T) {
	cols := []Column{
		{Name: "a", BitOffset: 0, BitWidth: 3},
		{Name: "b", BitOffset: 3, BitWidth: 7, Bias: 0},        // crosses the byte boundary
		{Name: "c", BitOffset: 10, BitWidth: 2, Bias: -2},      // biased into the null range
		{Name: "d", BitOffset: 0, BitWidth: 0, SymbolCount: 1}, // single-symbol field
	}
	// bits 0..2 = 5, bits 3..9 = 100, bits 10..11 = 1
	raw := make([]byte, 2)
	setBits := func(off, width int, v uint64) {
		for i := 0; i < width; i++ {
			if v&(1<<uint(i)) != 0 {
				raw[(off+i)/8] |= 1 << uint((off+i)%8)
			}
		}
	}
	setBits(0, 3, 5)
	setBits(3, 7, 100)
	setBits(10, 2, 1)

	out := make([]int64, len(cols))
	if err := DecodeRecord(raw, cols, out); err != nil {
		t.Fatalf("DecodeRecord: %v", err)
	}
	want := []int64{5, 100, NullSymbol, 0} // 1 + (-2) = -1 -> null
	for i := range want {
		if out[i] != want[i] {
			t.Errorf("column %q = %d, want %d", cols[i].Name, out[i], want[i])
		}
	}
}

func TestDecodeRecordBias(t *testing.T) {
	cols := []Column{{Name: "a", BitOffset: 0, BitWidth: 8, Bias: -1}}
	out := make([]int64, 1)
	for _, tc := range []struct{ raw, want int64 }{{0, NullSymbol}, {1, 0}, {200, 199}} {
		if err := DecodeRecord([]byte{byte(tc.raw)}, cols, out); err != nil {
			t.Fatal(err)
		}
		if out[0] != tc.want {
			t.Errorf("raw %d with bias -1 = %d, want %d", tc.raw, out[0], tc.want)
		}
	}
}

func TestDecodeRecordShortBuffer(t *testing.T) {
	cols := []Column{{Name: "wide", BitOffset: 0, BitWidth: 32}}
	err := DecodeRecord([]byte{1, 2}, cols, make([]int64, 1))
	if err == nil || !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("err = %v, want a bit-range error", err)
	}
	if err := DecodeRecord([]byte{1, 2, 3, 4}, cols, make([]int64, 0)); err == nil {
		t.Fatal("expected an error for an undersized out slice")
	}
}

func BenchmarkDecodeRecord(b *testing.B) {
	cols := make([]Column, 16)
	for i := range cols {
		cols[i] = Column{Name: "c", BitOffset: i * 7, BitWidth: 7}
	}
	raw := make([]byte, 16)
	out := make([]int64, len(cols))
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if err := DecodeRecord(raw, cols, out); err != nil {
			b.Fatal(err)
		}
	}
}

func TestDecodeRecordZeroWidthEmptySymbolTable(t *testing.T) {
	cols := []Column{{Name: "empty", BitWidth: 0, SymbolCount: 0}}
	out := make([]int64, 1)
	if err := DecodeRecord([]byte{0}, cols, out); err != nil {
		t.Fatal(err)
	}
	if out[0] != NullSymbol {
		t.Errorf("a zero-width column with no symbols = %d, want NullSymbol", out[0])
	}
}
