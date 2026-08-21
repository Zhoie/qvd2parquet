package qvd

import (
	"bytes"
	"errors"
	"strings"
	"testing"
)

const sampleHeader = `<?xml version="1.0" encoding="UTF-8"?>
<QvdTableHeader>
  <QvBuildNo>50000</QvBuildNo>
  <TableName>Sales</TableName>
  <Fields>
    <QvdFieldHeader>
      <FieldName>Id</FieldName>
      <BitOffset>0</BitOffset>
      <BitWidth>4</BitWidth>
      <Bias>0</Bias>
      <NumberFormat><Type>INTEGER</Type><nDec>0</nDec><UseThou>0</UseThou><Fmt></Fmt><Dec></Dec><Thou></Thou></NumberFormat>
      <NoOfSymbols>10</NoOfSymbols>
      <Offset>0</Offset>
      <Length>50</Length>
    </QvdFieldHeader>
    <QvdFieldHeader>
      <FieldName>Amount</FieldName>
      <BitOffset>4</BitOffset>
      <BitWidth>3</BitWidth>
      <Bias>-1</Bias>
      <NumberFormat><Type>MONEY</Type><nDec>2</nDec><UseThou>1</UseThou><Fmt>#.##0,00</Fmt><Dec>44</Dec><Thou>46</Thou></NumberFormat>
      <NoOfSymbols>5</NoOfSymbols>
      <Offset>50</Offset>
      <Length>60</Length>
    </QvdFieldHeader>
  </Fields>
  <RecordByteSize>1</RecordByteSize>
  <NoOfRecords>3</NoOfRecords>
  <Offset>110</Offset>
  <Length>3</Length>
</QvdTableHeader>`

func TestReadHeader(t *testing.T) {
	raw := append([]byte(sampleHeader), 0x00, 'X', 'Y')
	h, end, err := ReadHeader(bytes.NewReader(raw))
	if err != nil {
		t.Fatalf("ReadHeader: %v", err)
	}
	if want := int64(len(sampleHeader) + 1); end != want {
		t.Errorf("header end = %d, want %d", end, want)
	}
	if h.TableName != "Sales" {
		t.Errorf("TableName = %q", h.TableName)
	}
	if h.NoOfRecords != 3 || h.RecordByteSize != 1 {
		t.Errorf("NoOfRecords=%d RecordByteSize=%d", h.NoOfRecords, h.RecordByteSize)
	}
	if len(h.Fields) != 2 {
		t.Fatalf("got %d fields", len(h.Fields))
	}
	if err := h.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	cols := h.Columns()
	if cols[0].QlikType != QlikInteger || cols[1].QlikType != QlikMoney {
		t.Errorf("types = %v, %v", cols[0].QlikType, cols[1].QlikType)
	}
	if cols[1].Bias != -1 || cols[1].NDec != 2 {
		t.Errorf("Amount bias=%d nDec=%d", cols[1].Bias, cols[1].NDec)
	}
	// Dec=44 / Thou=46 are character codes for ',' and '.'.
	if cols[1].DecSep != "," || cols[1].ThouSep != "." {
		t.Errorf("separators = %q / %q", cols[1].DecSep, cols[1].ThouSep)
	}
}

func TestReadHeaderNoTerminator(t *testing.T) {
	if _, _, err := ReadHeader(strings.NewReader("<QvdTableHeader/>")); err == nil {
		t.Fatal("expected an error for a header without a 0x00 terminator")
	}
}

func TestHeaderMissingOptionalFields(t *testing.T) {
	const minimal = `<QvdTableHeader><TableName>T</TableName><Fields><QvdFieldHeader>` +
		`<FieldName>A</FieldName><BitOffset>0</BitOffset><BitWidth>0</BitWidth>` +
		`<NoOfSymbols>1</NoOfSymbols><Length>5</Length></QvdFieldHeader></Fields>` +
		`<RecordByteSize>1</RecordByteSize><NoOfRecords>1</NoOfRecords></QvdTableHeader>`
	h, err := ParseHeaderXML([]byte(minimal))
	if err != nil {
		t.Fatalf("ParseHeaderXML: %v", err)
	}
	if err := h.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	if got := h.Columns()[0].QlikType; got != QlikUnknown {
		t.Errorf("missing NumberFormat/Type should be QlikUnknown, got %v", got)
	}
}

func TestHeaderValidation(t *testing.T) {
	tests := []struct {
		name string
		mut  func(*TableHeader)
		want string
	}{
		{"no records but zero record size", func(h *TableHeader) { h.RecordByteSize = 0 }, "RecordByteSize"},
		{"negative bit offset", func(h *TableHeader) { h.Fields[0].BitOffset = -1 }, "negative BitOffset"},
		{"negative bit width", func(h *TableHeader) { h.Fields[0].BitWidth = -2 }, "negative BitWidth"},
		{"bit width over 64", func(h *TableHeader) { h.Fields[0].BitWidth = 65 }, "more than 64 bits"},
		{"overlapping ranges", func(h *TableHeader) { h.Fields[1].BitOffset = 3 }, "overlapping"},
		{"range past record", func(h *TableHeader) { h.Fields[1].BitWidth = 40 }, "exceeds RecordByteSize"},
		{"empty field name", func(h *TableHeader) { h.Fields[0].FieldName = "" }, "empty FieldName"},
		{"compression", func(h *TableHeader) { h.Compression = "1" }, "compression"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h, err := ParseHeaderXML([]byte(sampleHeader))
			if err != nil {
				t.Fatal(err)
			}
			tc.mut(h)
			err = h.Validate()
			if err == nil {
				t.Fatalf("expected validation error containing %q", tc.want)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error = %v, want it to contain %q", err, tc.want)
			}
		})
	}
}

func TestCompressionIsUnsupported(t *testing.T) {
	h, err := ParseHeaderXML([]byte(sampleHeader))
	if err != nil {
		t.Fatal(err)
	}
	h.Compression = "1"
	if err := h.Validate(); !errors.Is(err, ErrUnsupported) {
		t.Errorf("compression error = %v, want ErrUnsupported", err)
	}
}

func TestBitWidthZeroIsAllowed(t *testing.T) {
	h, err := ParseHeaderXML([]byte(sampleHeader))
	if err != nil {
		t.Fatal(err)
	}
	h.Fields[0].BitWidth = 0
	h.Fields[1].BitOffset = 0
	if err := h.Validate(); err != nil {
		t.Fatalf("BitWidth 0 should be allowed: %v", err)
	}
}
