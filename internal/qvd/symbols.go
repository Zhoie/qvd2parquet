package qvd

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
)

// Symbol table entry tags as written by Qlik.
const (
	tagNull      = 0x00
	tagInt       = 0x01
	tagDouble    = 0x02
	tagString    = 0x04
	tagDualInt   = 0x05
	tagDualFloat = 0x06
)

// UnknownSymbolPolicy selects the behaviour when an unrecognized symbol tag is
// encountered.
type UnknownSymbolPolicy int

const (
	// UnknownSymbolError aborts the conversion. This is the default.
	UnknownSymbolError UnknownSymbolPolicy = iota
	// UnknownSymbolEmpty substitutes an empty string and keeps reading.
	UnknownSymbolEmpty
)

// symbolReader decodes a stream of symbol table entries.
type symbolReader struct {
	br     *bufio.Reader
	policy UnknownSymbolPolicy
	buf8   [8]byte
	text   []byte
}

func newSymbolReader(r io.Reader, policy UnknownSymbolPolicy) *symbolReader {
	return &symbolReader{
		br:     bufio.NewReaderSize(r, 1<<20),
		policy: policy,
		text:   make([]byte, 0, 256),
	}
}

// readString consumes a zero-terminated UTF-8 string.
func (sr *symbolReader) readString() (string, error) {
	sr.text = sr.text[:0]
	for {
		b, err := sr.br.ReadByte()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return "", io.ErrUnexpectedEOF
			}
			return "", err
		}
		if b == 0x00 {
			return string(sr.text), nil
		}
		sr.text = append(sr.text, b)
	}
}

func (sr *symbolReader) readInt32() (int64, error) {
	if _, err := io.ReadFull(sr.br, sr.buf8[:4]); err != nil {
		return 0, err
	}
	return int64(int32(binary.LittleEndian.Uint32(sr.buf8[:4]))), nil
}

func (sr *symbolReader) readFloat64() (float64, error) {
	if _, err := io.ReadFull(sr.br, sr.buf8[:8]); err != nil {
		return 0, err
	}
	return math.Float64frombits(binary.LittleEndian.Uint64(sr.buf8[:8])), nil
}

// next decodes one symbol.
//
// Note on tags 0x05/0x06: the QVD format always stores the display string after
// the numeric payload, so both sides are consumed and preserved regardless of
// the field's declared type. The Java reference reader skips the string for
// INTEGER/REAL fields, which desynchronizes the stream; that behaviour is not
// reproduced here.
func (sr *symbolReader) next() (Symbol, error) {
	tag, err := sr.br.ReadByte()
	if err != nil {
		if errors.Is(err, io.EOF) {
			return Symbol{}, io.ErrUnexpectedEOF
		}
		return Symbol{}, err
	}
	switch tag {
	case tagNull:
		return Symbol{Kind: SymbolNull}, nil

	case tagInt:
		v, err := sr.readInt32()
		if err != nil {
			return Symbol{}, err
		}
		return Symbol{Kind: SymbolInt, Int: v}, nil

	case tagDouble:
		v, err := sr.readFloat64()
		if err != nil {
			return Symbol{}, err
		}
		return Symbol{Kind: SymbolFloat, Float: v}, nil

	case tagString:
		s, err := sr.readString()
		if err != nil {
			return Symbol{}, err
		}
		return Symbol{Kind: SymbolString, Text: s}, nil

	case tagDualInt:
		v, err := sr.readInt32()
		if err != nil {
			return Symbol{}, err
		}
		s, err := sr.readString()
		if err != nil {
			return Symbol{}, err
		}
		return Symbol{Kind: SymbolDualIntString, Int: v, Text: s}, nil

	case tagDualFloat:
		v, err := sr.readFloat64()
		if err != nil {
			return Symbol{}, err
		}
		s, err := sr.readString()
		if err != nil {
			return Symbol{}, err
		}
		return Symbol{Kind: SymbolDualFloatString, Float: v, Text: s}, nil

	default:
		if sr.policy == UnknownSymbolEmpty {
			return Symbol{Kind: SymbolString}, nil
		}
		return Symbol{}, fmt.Errorf("%w: unknown symbol tag 0x%02x", ErrUnsupported, tag)
	}
}

// ReadSymbolTable decodes exactly count symbols from r.
func ReadSymbolTable(r io.Reader, count int64, policy UnknownSymbolPolicy) ([]Symbol, *ColumnProfile, error) {
	sr := newSymbolReader(r, policy)
	syms := make([]Symbol, 0, count)
	prof := &ColumnProfile{}
	for i := int64(0); i < count; i++ {
		s, err := sr.next()
		if err != nil {
			return nil, nil, fmt.Errorf("symbol %d of %d: %w", i, count, err)
		}
		prof.Observe(s)
		syms = append(syms, s)
	}
	return syms, prof, nil
}
