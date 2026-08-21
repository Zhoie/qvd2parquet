package convert

import (
	"errors"
	"math/big"
	"testing"

	"github.com/ralforion/qvd2parquet/internal/qvd"
)

func TestScaledFromText(t *testing.T) {
	tests := []struct {
		text    string
		scale   int32
		decSep  string
		thouSep string
		want    string
		wantErr bool
	}{
		{"123.45", 2, ".", ",", "12345", false},
		{"1.234,56", 2, ",", ".", "123456", false},
		{"-10,50", 2, ",", ".", "-1050", false},
		{"1 234,50", 2, ",", " ", "123450", false},
		{"0", 2, ".", "", "0", false},
		{".5", 2, ".", "", "50", false},
		{"(12.34)", 2, ".", "", "-1234", false},
		{"+7", 3, ".", "", "7000", false},
		{"1.2300", 2, ".", "", "123", false}, // trailing zeros are droppable
		{"1.234", 2, ".", "", "", true},      // a real third decimal is not
		{"12,3 EUR", 2, ",", ".", "", true},  // suffixes are rejected
		{"", 2, ".", "", "", true},
		{"abc", 2, ".", "", "", true},
	}
	for _, tc := range tests {
		got, err := ScaledFromText(tc.text, tc.scale, tc.decSep, tc.thouSep)
		if tc.wantErr {
			if err == nil {
				t.Errorf("ScaledFromText(%q) = %v, want an error", tc.text, got)
			}
			continue
		}
		if err != nil {
			t.Errorf("ScaledFromText(%q): %v", tc.text, err)
			continue
		}
		if got.String() != tc.want {
			t.Errorf("ScaledFromText(%q, scale=%d) = %s, want %s", tc.text, tc.scale, got, tc.want)
		}
	}
}

func TestScaledFromTextTooManyDecimalsIsInexact(t *testing.T) {
	_, err := ScaledFromText("1.239", 2, ".", "")
	if !errors.Is(err, ErrDecimalInexact) {
		t.Fatalf("err = %v, want ErrDecimalInexact", err)
	}
}

func TestScaledFromFloat(t *testing.T) {
	tests := []struct {
		v       float64
		scale   int32
		want    string
		wantErr bool
	}{
		{123.45, 2, "12345", false},
		{-10.5, 2, "-1050", false},
		{0.1, 2, "10", false},  // 0.1 is not exact in binary; noise is rounded away
		{1.0 / 3, 2, "", true}, // genuinely more decimals than the scale
		{1.239, 2, "", true},
		{1e15, 2, "100000000000000000", false},
		{0, 4, "0", false},
	}
	for _, tc := range tests {
		got, err := ScaledFromFloat(tc.v, tc.scale)
		if tc.wantErr {
			if err == nil {
				t.Errorf("ScaledFromFloat(%v, %d) = %v, want an error", tc.v, tc.scale, got)
			} else if !errors.Is(err, ErrDecimalInexact) {
				t.Errorf("ScaledFromFloat(%v): err = %v, want ErrDecimalInexact", tc.v, err)
			}
			continue
		}
		if err != nil {
			t.Errorf("ScaledFromFloat(%v, %d): %v", tc.v, tc.scale, err)
			continue
		}
		if got.String() != tc.want {
			t.Errorf("ScaledFromFloat(%v, %d) = %s, want %s", tc.v, tc.scale, got, tc.want)
		}
	}
}

func TestDecimalExtractorPrefersText(t *testing.T) {
	// The binary side lost the intent; the display string kept it.
	ex := &DecimalExtractor{Scale: 2, Source: DecimalAuto, Strict: true, DecSep: ","}
	s := qvd.Symbol{Kind: qvd.SymbolDualFloatString, Float: 1.0 / 3, Text: "0,33"}
	got, err := ex.Scaled(s)
	if err != nil {
		t.Fatalf("Scaled: %v", err)
	}
	if got.String() != "33" {
		t.Errorf("scaled = %s, want 33", got)
	}
	if !ex.UsedText || ex.UsedNumeric {
		t.Errorf("UsedText=%v UsedNumeric=%v, want text only", ex.UsedText, ex.UsedNumeric)
	}
}

func TestDecimalExtractorFallsBackToNumeric(t *testing.T) {
	ex := &DecimalExtractor{Scale: 2, Source: DecimalAuto, Strict: true}
	got, err := ex.Scaled(qvd.Symbol{Kind: qvd.SymbolFloat, Float: 9.99})
	if err != nil {
		t.Fatalf("Scaled: %v", err)
	}
	if got.String() != "999" {
		t.Errorf("scaled = %s, want 999", got)
	}
	if !ex.UsedNumeric {
		t.Error("UsedNumeric should be set")
	}
}

func TestDecimalExtractorIntSymbol(t *testing.T) {
	ex := &DecimalExtractor{Scale: 3, Source: DecimalNumeric, Strict: true}
	got, err := ex.Scaled(qvd.Symbol{Kind: qvd.SymbolInt, Int: -12})
	if err != nil {
		t.Fatal(err)
	}
	if got.String() != "-12000" {
		t.Errorf("scaled = %s, want -12000", got)
	}
}

func TestDecimalExtractorNull(t *testing.T) {
	ex := &DecimalExtractor{Scale: 2, Source: DecimalAuto, Strict: true}
	got, err := ex.Scaled(qvd.Symbol{Kind: qvd.SymbolNull})
	if err != nil || got != nil {
		t.Errorf("null symbol -> %v, %v; want nil, nil", got, err)
	}
}

func TestDecimalExtractorTextSourceRequiresString(t *testing.T) {
	ex := &DecimalExtractor{Scale: 2, Source: DecimalText, Strict: true}
	if _, err := ex.Scaled(qvd.Symbol{Kind: qvd.SymbolFloat, Float: 1.5}); err == nil {
		t.Fatal("--decimal-source=text should reject a symbol without a display string")
	}
}

func TestResolveDecimalSpecPrecision(t *testing.T) {
	symbols := []qvd.Symbol{
		{Kind: qvd.SymbolFloat, Float: -99999.99},
		{Kind: qvd.SymbolFloat, Float: 1.25},
		{Kind: qvd.SymbolNull},
	}
	ex := &DecimalExtractor{Scale: 2, Source: DecimalNumeric, Strict: true}
	spec, scaled, err := ResolveDecimalSpec("Amount", symbols, ex)
	if err != nil {
		t.Fatalf("ResolveDecimalSpec: %v", err)
	}
	// -9999999 has 7 digits; the sign does not consume precision.
	if spec.Precision != 7 || spec.Scale != 2 {
		t.Errorf("spec = %v, want decimal(7,2)", spec)
	}
	if scaled[0].String() != "-9999999" || scaled[2] != nil {
		t.Errorf("scaled = %v", scaled)
	}
}

func TestResolveDecimalSpecFailsOnInexact(t *testing.T) {
	symbols := []qvd.Symbol{{Kind: qvd.SymbolFloat, Float: 1.0 / 3}}
	ex := &DecimalExtractor{Scale: 2, Source: DecimalNumeric, Strict: true}
	_, _, err := ResolveDecimalSpec("Rate", symbols, ex)
	if err == nil {
		t.Fatal("expected a strict-mode failure")
	}
	if !errors.Is(err, ErrDecimalInexact) {
		t.Errorf("err = %v, want ErrDecimalInexact", err)
	}
}

func TestResolveDecimalSpecPrecisionOverflow(t *testing.T) {
	huge, _ := new(big.Float).SetString("1e40")
	f, _ := huge.Float64()
	symbols := []qvd.Symbol{{Kind: qvd.SymbolFloat, Float: f}}
	ex := &DecimalExtractor{Scale: 2, Source: DecimalNumeric, Strict: true}
	if _, _, err := ResolveDecimalSpec("Big", symbols, ex); err == nil {
		t.Fatal("expected a precision overflow error")
	}
}

func TestInferScaleFromSymbols(t *testing.T) {
	symbols := []qvd.Symbol{
		{Kind: qvd.SymbolDualFloatString, Text: "1,5"},
		{Kind: qvd.SymbolDualFloatString, Text: "10,125"},
		{Kind: qvd.SymbolDualFloatString, Text: "3"},
	}
	scale, ok := InferScaleFromSymbols(symbols, ",")
	if !ok || scale != 3 {
		t.Errorf("scale = %d, ok = %v; want 3, true", scale, ok)
	}
	if _, ok := InferScaleFromSymbols([]qvd.Symbol{{Kind: qvd.SymbolFloat}}, "."); ok {
		t.Error("a column with no display strings should not yield a scale")
	}
}

func TestFormatScaled(t *testing.T) {
	tests := []struct {
		v     string
		scale int32
		want  string
	}{
		{"12345", 2, "123.45"},
		{"-1050", 2, "-10.50"},
		{"5", 3, "0.005"},
		{"-5", 3, "-0.005"},
		{"0", 2, "0.00"},
		{"999", 0, "999"},
	}
	for _, tc := range tests {
		v, _ := new(big.Int).SetString(tc.v, 10)
		if got := FormatScaled(v, tc.scale); got != tc.want {
			t.Errorf("FormatScaled(%s, %d) = %q, want %q", tc.v, tc.scale, got, tc.want)
		}
	}
	if got := FormatScaled(nil, 2); got != "" {
		t.Errorf("FormatScaled(nil) = %q, want empty", got)
	}
}
