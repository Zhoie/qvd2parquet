package convert

import (
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"os"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/ralforion/qvd2parquet/internal/qvd"
)

// ErrSchemaPolicy marks a schema/type policy failure (CLI exit code 3).
var ErrSchemaPolicy = errors.New("schema/type policy error")

// ValueStrategy selects how a symbol is turned into an output value.
type ValueStrategy int

const (
	// StrategyNull emits nulls only; used for all-null columns.
	StrategyNull ValueStrategy = iota
	// StrategyString writes the display string, formatting numerics when needed.
	StrategyString
	// StrategyInt64 writes the integer numeric side.
	StrategyInt64
	// StrategyFloat64 writes the numeric side as a double.
	StrategyFloat64
	// StrategyDate32 writes days since the Unix epoch.
	StrategyDate32
	// StrategyTimestampMillis writes milliseconds since the Unix epoch.
	StrategyTimestampMillis
	// StrategyTimeMillis writes milliseconds since midnight.
	StrategyTimeMillis
	// StrategyDecimal writes an exact scaled decimal.
	StrategyDecimal
	// StrategyDualText writes the display side of a dual into a companion column.
	StrategyDualText
)

func (s ValueStrategy) String() string {
	return [...]string{
		"StrategyNull", "StrategyString", "StrategyInt64", "StrategyFloat64",
		"StrategyDate32", "StrategyTimestampMillis", "StrategyTimeMillis",
		"StrategyDecimal", "StrategyDualText",
	}[s]
}

// IsNumericAggregatable reports whether numeric quality metrics apply.
func (s ValueStrategy) IsNumericAggregatable() bool {
	switch s {
	case StrategyInt64, StrategyFloat64, StrategyDate32, StrategyTimestampMillis,
		StrategyTimeMillis, StrategyDecimal:
		return true
	}
	return false
}

// ResolvedColumn is one output Parquet column.
type ResolvedColumn struct {
	// SourceIndex is the index of the QVD field this column reads from.
	SourceIndex int
	Name        string
	ArrowType   arrow.DataType
	Nullable    bool
	Strategy    ValueStrategy
	// Decimal is set when Strategy is StrategyDecimal.
	Decimal DecimalSpec
	// Scaled holds the pre-converted scaled decimal per symbol index, so record
	// decoding never re-parses a symbol. Only set for StrategyDecimal.
	Scaled []*big.Int
	// DecimalFromText and DecimalFromNumeric record where digits came from.
	DecimalFromText    bool
	DecimalFromNumeric bool
}

// ResolvedSchema is the full output schema plus the reasoning behind it.
type ResolvedSchema struct {
	Columns []ResolvedColumn
	Arrow   *arrow.Schema
	// Notes explains, per source column, how the type was chosen.
	Notes []string
}

// SchemaOverride is the --schema JSON document.
type SchemaOverride struct {
	Columns map[string]ColumnOverride `json:"columns"`
}

// ColumnOverride pins one column's output type.
type ColumnOverride struct {
	Type      string `json:"type"`
	Precision int32  `json:"precision"`
	Scale     int32  `json:"scale"`
}

// LoadSchemaOverride reads and validates a --schema document.
func LoadSchemaOverride(path string) (*SchemaOverride, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read schema override %s: %w", path, err)
	}
	var so SchemaOverride
	dec := json.NewDecoder(strings.NewReader(string(b)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&so); err != nil {
		return nil, fmt.Errorf("parse schema override %s: %w", path, err)
	}
	for name, co := range so.Columns {
		switch strings.ToLower(co.Type) {
		case "string", "int64", "float64", "date32", "timestamp", "time", "decimal":
		default:
			return nil, fmt.Errorf("schema override for %q: unknown type %q "+
				"(want string|int64|float64|date32|timestamp|time|decimal)", name, co.Type)
		}
		if strings.EqualFold(co.Type, "decimal") {
			if co.Scale < 0 || co.Precision <= 0 || co.Precision > maxDecimal128Precision {
				return nil, fmt.Errorf("schema override for %q: decimal needs 0 < precision <= %d and scale >= 0, got precision=%d scale=%d",
					name, maxDecimal128Precision, co.Precision, co.Scale)
			}
			if co.Scale > co.Precision {
				return nil, fmt.Errorf("schema override for %q: scale %d exceeds precision %d", name, co.Scale, co.Precision)
			}
		}
	}
	return &so, nil
}

func (so *SchemaOverride) lookup(name string) (ColumnOverride, bool) {
	if so == nil {
		return ColumnOverride{}, false
	}
	if co, ok := so.Columns[name]; ok {
		return co, true
	}
	for k, co := range so.Columns {
		if strings.EqualFold(k, name) {
			return co, true
		}
	}
	return ColumnOverride{}, false
}

// Arrow type constructors used by the resolver.
var (
	arrowInt64  = arrow.PrimitiveTypes.Int64
	arrowF64    = arrow.PrimitiveTypes.Float64
	arrowString = arrow.BinaryTypes.String
	arrowDate32 = arrow.FixedWidthTypes.Date32
	arrowTime32 = arrow.FixedWidthTypes.Time32ms
)

// ResolveSchema turns profiled QVD columns into the output Parquet schema.
func ResolveSchema(f *qvd.File, opts *Options, override *SchemaOverride) (*ResolvedSchema, error) {
	rs := &ResolvedSchema{}
	tsType := &arrow.TimestampType{Unit: arrow.Millisecond, TimeZone: arrowTimeZoneName(opts)}

	for _, idx := range f.SelectedColumns() {
		col := f.Columns[idx]
		prof := f.Profiles[idx]
		syms := f.Symbols[idx]

		cols, note, err := resolveColumn(col, prof, syms, opts, override, tsType)
		if err != nil {
			return nil, err
		}
		rs.Columns = append(rs.Columns, cols...)
		rs.Notes = append(rs.Notes, note)
	}

	fields := make([]arrow.Field, len(rs.Columns))
	for i, c := range rs.Columns {
		fields[i] = arrow.Field{Name: c.Name, Type: c.ArrowType, Nullable: c.Nullable}
	}
	rs.Arrow = arrow.NewSchema(fields, nil)
	return rs, nil
}

// arrowTimeZoneName is the timezone stamped into the Arrow timestamp type so
// downstream readers interpret the stored UTC milliseconds correctly.
func arrowTimeZoneName(opts *Options) string {
	if opts.Location == nil {
		return "UTC"
	}
	return opts.Location.String()
}

// resolveColumn decides the output column(s) for one QVD field. It returns one
// column normally, two when a dual is split.
func resolveColumn(col qvd.Column, prof *qvd.ColumnProfile, syms []qvd.Symbol,
	opts *Options, override *SchemaOverride, tsType arrow.DataType) ([]ResolvedColumn, string, error) {

	base := ResolvedColumn{SourceIndex: col.Index, Name: col.Name, Nullable: true}

	// An explicit override wins over inference, but is still validated against
	// the symbols actually present.
	if co, ok := override.lookup(col.Name); ok {
		rc, err := applyOverride(base, co, col, syms)
		if err != nil {
			return nil, "", err
		}
		return []ResolvedColumn{rc}, fmt.Sprintf("%s: pinned to %s by --schema", col.Name, rc.ArrowType), nil
	}

	if prof.HasOnlyNulls() {
		base.ArrowType, base.Strategy = arrowString, StrategyNull
		return []ResolvedColumn{base}, fmt.Sprintf("%s: all %d symbols are null, written as nullable utf8",
			col.Name, prof.Symbols), nil
	}

	// A column mixing plain strings with numerics is the dangerous case.
	if prof.HasMixedScalarFamilies() {
		switch {
		case opts.Mixed == MixedString || opts.MixedStringFallback:
			base.ArrowType, base.Strategy = arrowString, StrategyString
			return []ResolvedColumn{base}, fmt.Sprintf(
				"%s: mixed text and numeric symbols (%s), written as utf8", col.Name, prof.Describe()), nil
		default:
			return nil, "", fmt.Errorf("%w: mixed type column %q: symbols contain %d numeric values and %d strings; "+
				"use --mixed=string to write this column as UTF-8, or --mixed-string-fallback",
				ErrSchemaPolicy, col.Name, prof.Numeric(), prof.Strings)
		}
	}

	// Pure text.
	if prof.HasOnlyText() {
		base.ArrowType, base.Strategy = arrowString, StrategyString
		return []ResolvedColumn{base}, fmt.Sprintf("%s: %d text symbols, written as utf8", col.Name, prof.Strings), nil
	}

	// From here on the column is numeric or dual-numeric only.
	dual := prof.HasDuals()

	// --mixed=string converts every column to text on request.
	if opts.Mixed == MixedString {
		base.ArrowType, base.Strategy = arrowString, StrategyString
		return []ResolvedColumn{base}, fmt.Sprintf("%s: --mixed=string, written as utf8", col.Name), nil
	}

	// A dual whose text side is requested becomes a plain string column.
	if dual && opts.Dual == DualText {
		base.ArrowType, base.Strategy = arrowString, StrategyString
		return []ResolvedColumn{base}, fmt.Sprintf(
			"%s: dual column, --dual=text selects the display side (utf8)", col.Name), nil
	}

	numeric, note, err := resolveNumericColumn(base, col, prof, syms, opts, tsType)
	if err != nil {
		return nil, "", err
	}
	out := []ResolvedColumn{numeric}

	if dual && opts.Dual == DualColumns {
		text := ResolvedColumn{
			SourceIndex: col.Index,
			Name:        col.Name + "__text",
			ArrowType:   arrowString,
			Nullable:    true,
			Strategy:    StrategyDualText,
		}
		out = append(out, text)
		note += fmt.Sprintf("; display side written to %q", text.Name)
	}
	return out, note, nil
}

// resolveNumericColumn picks the typed representation for a numeric column.
func resolveNumericColumn(base ResolvedColumn, col qvd.Column, prof *qvd.ColumnProfile,
	syms []qvd.Symbol, opts *Options, tsType arrow.DataType) (ResolvedColumn, string, error) {

	switch col.QlikType {
	case qvd.QlikDate:
		base.ArrowType, base.Strategy = arrowDate32, StrategyDate32
		return base, fmt.Sprintf("%s: DATE, written as date32 (days since epoch)", col.Name), nil

	case qvd.QlikTimestamp:
		base.ArrowType, base.Strategy = tsType, StrategyTimestampMillis
		return base, fmt.Sprintf("%s: TIMESTAMP, written as timestamp[ms, tz=%s]",
			col.Name, tsType.(*arrow.TimestampType).TimeZone), nil

	case qvd.QlikTime:
		base.ArrowType, base.Strategy = arrowTime32, StrategyTimeMillis
		return base, fmt.Sprintf("%s: TIME, written as time32[ms] (milliseconds since midnight)", col.Name), nil

	case qvd.QlikFix, qvd.QlikMoney:
		return resolveDecimalColumn(base, col, syms, opts)
	}

	// INTEGER, REAL, ASCII-with-numbers and UNKNOWN fall through to the
	// profile-driven choice.
	switch {
	case prof.HasOnlyInts():
		base.ArrowType, base.Strategy = arrowInt64, StrategyInt64
		return base, fmt.Sprintf("%s: %s with %d integer symbols, written as int64",
			col.Name, col.QlikType, prof.IntLike()), nil

	case prof.HasOnlyFloats():
		base.ArrowType, base.Strategy = arrowF64, StrategyFloat64
		return base, fmt.Sprintf("%s: %s with %d double symbols, written as float64",
			col.Name, col.QlikType, prof.FloatLike()), nil

	case prof.CanPromoteIntToFloat():
		allowed := opts.NumericPromote &&
			(opts.Mixed == MixedPromote || opts.Mixed == MixedError || opts.Mixed == MixedDualColumns)
		if !allowed {
			return ResolvedColumn{}, "", fmt.Errorf(
				"%w: mixed type column %q: symbols contain %d integers and %d doubles; "+
					"enable --numeric-promote to widen the column to float64, or use --mixed=string",
				ErrSchemaPolicy, col.Name, prof.IntLike(), prof.FloatLike())
		}
		base.ArrowType, base.Strategy = arrowF64, StrategyFloat64
		return base, fmt.Sprintf("%s: %d integer and %d double symbols promoted to float64",
			col.Name, prof.IntLike(), prof.FloatLike()), nil
	}

	return ResolvedColumn{}, "", fmt.Errorf("%w: column %q: cannot resolve a type from %s",
		ErrSchemaPolicy, col.Name, prof.Describe())
}

// resolveDecimalColumn resolves MONEY/FIX to an exact Parquet decimal.
func resolveDecimalColumn(base ResolvedColumn, col qvd.Column, syms []qvd.Symbol, opts *Options) (ResolvedColumn, string, error) {
	scale := int32(col.NDec)
	scaleSource := "NumberFormat/nDec"
	if col.NDec <= 0 {
		inferred, ok := InferScaleFromSymbols(syms, col.DecSep)
		if !ok {
			if col.NDec == 0 {
				// nDec is genuinely zero and no display strings contradict it.
				scale, scaleSource = 0, "NumberFormat/nDec (0)"
			} else {
				return ResolvedColumn{}, "", fmt.Errorf(
					"%w: column %q is %s but neither NumberFormat/nDec nor display strings provide a decimal scale; "+
						"pin it with --schema {\"columns\":{%q:{\"type\":\"decimal\",\"precision\":18,\"scale\":2}}}",
					ErrSchemaPolicy, col.Name, col.QlikType, col.Name)
			}
		} else {
			scale, scaleSource = inferred, "inferred from display strings"
		}
	}

	ex := &DecimalExtractor{
		Scale:   scale,
		Source:  opts.DecimalSource,
		Strict:  opts.DecimalStrict,
		DecSep:  col.DecSep,
		ThouSep: col.ThouSep,
	}
	spec, scaled, err := ResolveDecimalSpec(col.Name, syms, ex)
	if err != nil {
		return ResolvedColumn{}, "", fmt.Errorf("%w: %s", ErrSchemaPolicy, err)
	}

	base.ArrowType = &arrow.Decimal128Type{Precision: spec.Precision, Scale: spec.Scale}
	base.Strategy = StrategyDecimal
	base.Decimal = spec
	base.Scaled = scaled
	base.DecimalFromText = ex.UsedText
	base.DecimalFromNumeric = ex.UsedNumeric

	src := "no values"
	switch {
	case ex.UsedText && ex.UsedNumeric:
		src = "dual display strings and numeric payloads"
	case ex.UsedText:
		src = "dual display strings"
	case ex.UsedNumeric:
		src = "numeric payloads"
	}
	return base, fmt.Sprintf("%s: %s, written as %s; scale from %s, digits from %s",
		col.Name, col.QlikType, spec, scaleSource, src), nil
}

// applyOverride pins a column's type and validates it against the symbols.
func applyOverride(base ResolvedColumn, co ColumnOverride, col qvd.Column, syms []qvd.Symbol) (ResolvedColumn, error) {
	switch strings.ToLower(co.Type) {
	case "string":
		base.ArrowType, base.Strategy = arrowString, StrategyString
	case "int64":
		for i, s := range syms {
			if s.Kind == qvd.SymbolNull {
				continue
			}
			if s.Kind == qvd.SymbolFloat || s.Kind == qvd.SymbolDualFloatString {
				return base, fmt.Errorf("%w: schema override pins %q to int64, but symbol %d is a double (%v)",
					ErrSchemaPolicy, col.Name, i, s.Float)
			}
			if s.Kind == qvd.SymbolString {
				return base, fmt.Errorf("%w: schema override pins %q to int64, but symbol %d is the string %q",
					ErrSchemaPolicy, col.Name, i, s.Text)
			}
		}
		base.ArrowType, base.Strategy = arrowInt64, StrategyInt64
	case "float64":
		for i, s := range syms {
			if s.Kind == qvd.SymbolString {
				return base, fmt.Errorf("%w: schema override pins %q to float64, but symbol %d is the string %q",
					ErrSchemaPolicy, col.Name, i, s.Text)
			}
		}
		base.ArrowType, base.Strategy = arrowF64, StrategyFloat64
	case "date32":
		base.ArrowType, base.Strategy = arrowDate32, StrategyDate32
	case "timestamp":
		base.ArrowType, base.Strategy = &arrow.TimestampType{Unit: arrow.Millisecond, TimeZone: "UTC"}, StrategyTimestampMillis
	case "time":
		base.ArrowType, base.Strategy = arrowTime32, StrategyTimeMillis
	case "decimal":
		ex := &DecimalExtractor{
			Scale:   co.Scale,
			Source:  DecimalAuto,
			Strict:  true,
			DecSep:  col.DecSep,
			ThouSep: col.ThouSep,
		}
		spec, scaled, err := ResolveDecimalSpec(col.Name, syms, ex)
		if err != nil {
			return base, fmt.Errorf("%w: %s", ErrSchemaPolicy, err)
		}
		if spec.Precision > co.Precision {
			return base, fmt.Errorf("%w: schema override pins %q to decimal(%d,%d), but the data needs precision %d",
				ErrSchemaPolicy, col.Name, co.Precision, co.Scale, spec.Precision)
		}
		spec.Precision = co.Precision
		base.ArrowType = &arrow.Decimal128Type{Precision: spec.Precision, Scale: spec.Scale}
		base.Strategy, base.Decimal, base.Scaled = StrategyDecimal, spec, scaled
		base.DecimalFromText, base.DecimalFromNumeric = ex.UsedText, ex.UsedNumeric
	}
	return base, nil
}
