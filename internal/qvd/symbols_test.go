package qvd

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math"
	"testing"
)

func intSym(v int32) []byte {
	b := []byte{tagInt, 0, 0, 0, 0}
	binary.LittleEndian.PutUint32(b[1:], uint32(v))
	return b
}

func floatSym(v float64) []byte {
	b := append([]byte{tagDouble}, make([]byte, 8)...)
	binary.LittleEndian.PutUint64(b[1:], math.Float64bits(v))
	return b
}

func strSym(s string) []byte {
	return append(append([]byte{tagString}, s...), 0x00)
}

func dualIntSym(v int32, s string) []byte {
	b := []byte{tagDualInt, 0, 0, 0, 0}
	binary.LittleEndian.PutUint32(b[1:], uint32(v))
	return append(append(b, s...), 0x00)
}

func dualFloatSym(v float64, s string) []byte {
	b := append([]byte{tagDualFloat}, make([]byte, 8)...)
	binary.LittleEndian.PutUint64(b[1:], math.Float64bits(v))
	return append(append(b, s...), 0x00)
}

func TestReadSymbolTableAllTags(t *testing.T) {
	var buf bytes.Buffer
	buf.Write([]byte{tagNull})
	buf.Write(intSym(-42))
	buf.Write(floatSym(3.5))
	buf.Write(strSym("héllo"))
	buf.Write(dualIntSym(7, "007"))
	buf.Write(dualFloatSym(-1.25, "-1,25"))

	syms, prof, err := ReadSymbolTable(&buf, 6, UnknownSymbolError)
	if err != nil {
		t.Fatalf("ReadSymbolTable: %v", err)
	}
	want := []Symbol{
		{Kind: SymbolNull},
		{Kind: SymbolInt, Int: -42},
		{Kind: SymbolFloat, Float: 3.5},
		{Kind: SymbolString, Text: "héllo"},
		{Kind: SymbolDualIntString, Int: 7, Text: "007"},
		{Kind: SymbolDualFloatString, Float: -1.25, Text: "-1,25"},
	}
	for i := range want {
		if syms[i] != want[i] {
			t.Errorf("symbol %d = %+v, want %+v", i, syms[i], want[i])
		}
	}
	if buf.Len() != 0 {
		t.Errorf("%d bytes left unconsumed", buf.Len())
	}
	if prof.Nulls != 1 || prof.Ints != 1 || prof.Floats != 1 || prof.Strings != 1 ||
		prof.DualInts != 1 || prof.DualFloats != 1 || prof.Symbols != 6 {
		t.Errorf("profile = %+v", prof)
	}
	if prof.MinInt != -42 || prof.MaxInt != 7 {
		t.Errorf("int range = [%d,%d], want [-42,7]", prof.MinInt, prof.MaxInt)
	}
	if prof.MinFloat != -1.25 || prof.MaxFloat != 3.5 {
		t.Errorf("float range = [%v,%v]", prof.MinFloat, prof.MaxFloat)
	}
}

// Tags 0x05 and 0x06 always carry a display string after the numeric payload,
// even for INTEGER/REAL fields. Skipping it would desynchronize the stream.
func TestDualSymbolsAlwaysConsumeTheirString(t *testing.T) {
	var buf bytes.Buffer
	buf.Write(dualIntSym(1, "one"))
	buf.Write(intSym(2))
	syms, _, err := ReadSymbolTable(&buf, 2, UnknownSymbolError)
	if err != nil {
		t.Fatalf("ReadSymbolTable: %v", err)
	}
	if syms[1].Kind != SymbolInt || syms[1].Int != 2 {
		t.Errorf("second symbol = %+v, want int 2 (stream desynchronized)", syms[1])
	}
}

func TestReadSymbolTableEmptyString(t *testing.T) {
	syms, prof, err := ReadSymbolTable(bytes.NewReader(strSym("")), 1, UnknownSymbolError)
	if err != nil {
		t.Fatal(err)
	}
	if syms[0].Kind != SymbolString || syms[0].Text != "" {
		t.Errorf("symbol = %+v", syms[0])
	}
	if prof.EmptyText != 1 {
		t.Errorf("EmptyText = %d, want 1", prof.EmptyText)
	}
}

func TestReadSymbolTableUnknownTag(t *testing.T) {
	_, _, err := ReadSymbolTable(bytes.NewReader([]byte{0x03}), 1, UnknownSymbolError)
	if !errors.Is(err, ErrUnsupported) {
		t.Fatalf("err = %v, want ErrUnsupported", err)
	}

	syms, _, err := ReadSymbolTable(bytes.NewReader([]byte{0x03}), 1, UnknownSymbolEmpty)
	if err != nil {
		t.Fatalf("UnknownSymbolEmpty: %v", err)
	}
	if syms[0].Kind != SymbolString || syms[0].Text != "" {
		t.Errorf("symbol = %+v, want empty string", syms[0])
	}
}

func TestReadSymbolTableTruncated(t *testing.T) {
	_, _, err := ReadSymbolTable(bytes.NewReader([]byte{tagInt, 0x01, 0x02}), 1, UnknownSymbolError)
	if err == nil {
		t.Fatal("expected an error for a truncated integer symbol")
	}
	_, _, err = ReadSymbolTable(bytes.NewReader([]byte{tagString, 'a', 'b'}), 1, UnknownSymbolError)
	if err == nil {
		t.Fatal("expected an error for an unterminated string symbol")
	}
}

func TestProfileHelpers(t *testing.T) {
	tests := []struct {
		name    string
		symbols []Symbol
		check   func(*ColumnProfile) bool
	}{
		{"only nulls", []Symbol{{Kind: SymbolNull}}, (*ColumnProfile).HasOnlyNulls},
		{"only text", []Symbol{{Kind: SymbolString}, {Kind: SymbolNull}}, (*ColumnProfile).HasOnlyText},
		{"only ints", []Symbol{{Kind: SymbolInt}, {Kind: SymbolDualIntString}}, (*ColumnProfile).HasOnlyInts},
		{"only floats", []Symbol{{Kind: SymbolFloat}}, (*ColumnProfile).HasOnlyFloats},
		{"promotable", []Symbol{{Kind: SymbolInt}, {Kind: SymbolFloat}}, (*ColumnProfile).CanPromoteIntToFloat},
		{"mixed families", []Symbol{{Kind: SymbolInt}, {Kind: SymbolString}}, (*ColumnProfile).HasMixedScalarFamilies},
		{"duals", []Symbol{{Kind: SymbolDualFloatString}}, (*ColumnProfile).HasDuals},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			p := &ColumnProfile{}
			for _, s := range tc.symbols {
				p.Observe(s)
			}
			if !tc.check(p) {
				t.Errorf("predicate false for %+v", p)
			}
		})
	}
}
