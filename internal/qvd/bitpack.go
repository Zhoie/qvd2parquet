package qvd

import "fmt"

// NullSymbol marks a record field whose biased symbol index is negative.
const NullSymbol = int64(-1)

// readBitsLE extracts bitWidth bits starting at bitOffset from raw, where bit
// n lives in raw[n/8] at position n%8 counting from the least significant bit,
// and increasing bit positions carry increasing significance.
//
// The caller must guarantee that the range fits inside raw; DecodeRecord
// enforces this once per record via the precomputed column layout.
func readBitsLE(raw []byte, bitOffset, bitWidth int) uint64 {
	if bitWidth == 0 {
		return 0
	}
	byteIdx := bitOffset >> 3
	shift := uint(bitOffset & 7)

	// Load the up-to-9 bytes that can hold shift+bitWidth <= 71 bits.
	var lo uint64
	n := len(raw) - byteIdx
	if n > 8 {
		n = 8
	}
	for i := 0; i < n; i++ {
		lo |= uint64(raw[byteIdx+i]) << (8 * uint(i))
	}
	v := lo >> shift
	if shift > 0 && byteIdx+8 < len(raw) {
		v |= uint64(raw[byteIdx+8]) << (64 - shift)
	}
	if bitWidth < 64 {
		v &= (uint64(1) << uint(bitWidth)) - 1
	}
	return v
}

// DecodeRecord resolves the symbol index of every column in cols from one
// fixed-width record. Indices are written to out, which must have len(cols)
// entries. A negative biased index yields NullSymbol.
func DecodeRecord(raw []byte, cols []Column, out []int64) error {
	if len(out) < len(cols) {
		return fmt.Errorf("decode record: out has %d slots for %d columns", len(out), len(cols))
	}
	for i := range cols {
		c := &cols[i]
		if c.BitWidth == 0 {
			// No record bits: the field holds at most one symbol, so every
			// row points at index 0 unless the symbol table is empty.
			if c.SymbolCount == 0 {
				out[i] = NullSymbol
			} else {
				out[i] = 0
			}
			continue
		}
		if end := c.BitOffset + c.BitWidth; end > len(raw)*8 {
			return fmt.Errorf("column %q bit range [%d,%d) exceeds the %d-byte record",
				c.Name, c.BitOffset, end, len(raw))
		}
		idx := int64(readBitsLE(raw, c.BitOffset, c.BitWidth)) + c.Bias
		if idx < 0 {
			out[i] = NullSymbol
		} else {
			out[i] = idx
		}
	}
	return nil
}
