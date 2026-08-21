# qvd2parquet Implementation Plan

## Goal

Build a fast Go command-line program that converts large Qlik QVD files into Parquet files.

The first deliverable is a production-oriented converter, not a reusable library. The internal code should still be organized cleanly enough that parser, profiling, and writer pieces can be tested independently and promoted to library packages later if needed.

Primary command:

```sh
qvd2parquet input.qvd output.parquet
```

Primary requirements:

- Read standard, unencrypted QVD files.
- Preserve useful Parquet types instead of converting everything to strings.
- Preserve exact decimal values for QVD decimal-like fields, especially `MONEY` and `FIX`.
- Detect columns with multiple physical or semantic data types after reading symbol tables.
- Apply an explicit, configurable strategy for mixed-type columns.
- Decode records in parallel from the first implementation.
- Stream decoded records into Parquet row groups so large files do not require materializing all rows.
- Provide predictable schemas and useful failure messages for ETL usage.

## Reference Implementation

Use the existing Java reader in `../qvd-reader` as the behavioral reference:

- Header read boundary: `src/main/java/bi/irregular/data/conversion/QvdReader.java`, constructor reads bytes until `0x00`.
- Header fields: `readHeader()` extracts field name, field type, bit offset, bit width, bias, symbol count, offset, and length.
- Symbol tables: `readSymbols()` decodes byte-stuffed symbols.
- Records: `getRecord()` reads fixed-size bit-stuffed records and resolves symbol IDs.
- Bit decoding: `decodeBitStuffedRecord()` decodes little-endian bit ranges.
- Date/time conversion: `getDateTimeFromDouble()` implements Qlik serial date/time conversion.

Do not port the Java licensing/demo-limit code. This tool should have no row limit.

## Target Stack

Language:

- Go 1.22 or newer.

Parquet and Arrow:

- Preferred: `github.com/apache/arrow-go/v18`
- Use Arrow builders to accumulate batches.
- Use `parquet/pqarrow.FileWriter` to write Arrow records to Parquet.

Compression:

- Default: `zstd` if supported and stable in the selected Arrow Go version.
- Fallback/default alternative: `snappy`.
- CLI override: `--compression zstd|snappy|gzip|uncompressed`.

CLI:

- Use standard `flag` initially unless subcommands are needed.
- Keep dependencies low until the converter behavior stabilizes.

## High-Level Pipeline

1. Open QVD input.
2. Read XML header bytes until the first `0x00`.
3. Unmarshal the needed XML fields into Go structs.
4. Build per-column metadata from the header.
5. Read symbol tables for all selected columns.
6. Store each symbol as a typed value, not only as text.
7. Profile actual symbol types for each column.
8. Resolve a deterministic Arrow/Parquet schema using the configured type policy.
9. Create the Parquet writer.
10. Decode QVD records in parallel chunks:
    - assign contiguous row ranges to worker goroutines,
    - read fixed-width record bytes with `ReadAt`,
    - decode symbol IDs,
    - apply field bias and null handling,
    - resolve symbol values,
    - append typed values to worker-local Arrow builders,
    - collect worker-local quality metrics.
11. Deliver completed chunks to a single writer as workers finish; QVD source row order is not preserved by default.
12. Flush each completed Arrow record as one Parquet row group or writer batch.
13. Merge quality metrics and run the optional quality gate.
14. Close the Parquet writer and report conversion statistics.

## Repository Layout

Proposed initial layout:

```text
.
├── cmd/
│   └── qvd2parquet/
│       └── main.go
├── internal/
│   ├── qvd/
│   │   ├── header.go
│   │   ├── reader.go
│   │   ├── symbols.go
│   │   ├── bitpack.go
│   │   ├── types.go
│   │   └── time.go
│   ├── convert/
│   │   ├── schema.go
│   │   ├── policy.go
│   │   ├── batch.go
│   │   ├── parallel.go
│   │   ├── decimal.go
│   │   ├── quality.go
│   │   └── converter.go
│   └── parquetwrite/
│       └── writer.go
├── testdata/
│   └── README.md
├── IMPLEMENTATION_PLAN.md
├── go.mod
└── README.md
```

Package responsibilities:

- `internal/qvd`: QVD format parsing only. No Parquet dependency.
- `internal/convert`: schema resolution, type policy, and row conversion.
- `internal/parquetwrite`: Arrow/Parquet writer integration.
- `cmd/qvd2parquet`: CLI argument parsing, progress logging, and orchestration.

## QVD Header Parsing

The QVD file starts with a UTF-8 XML header terminated by a null byte.

Implementation:

- Use `bufio.Reader` or direct `io.Reader` loop to read bytes until `0x00`.
- Track the byte offset immediately after the null byte; symbol tables begin there.
- Use `encoding/xml` to unmarshal only required elements.

Minimum header structs:

```go
type TableHeader struct {
    XMLName        xml.Name `xml:"QvdTableHeader"`
    TableName      string   `xml:"TableName"`
    RecordByteSize int      `xml:"RecordByteSize"`
    NoOfRecords    int64    `xml:"NoOfRecords"`
    Fields         []FieldHeader `xml:"Fields>QvdFieldHeader"`
}

type FieldHeader struct {
    FieldName    string       `xml:"FieldName"`
    BitOffset    int          `xml:"BitOffset"`
    BitWidth     int          `xml:"BitWidth"`
    Bias         int64        `xml:"Bias"`
    NumberFormat NumberFormat `xml:"NumberFormat"`
    NoOfSymbols  int64        `xml:"NoOfSymbols"`
    Offset       int64        `xml:"Offset"`
    Length       int64        `xml:"Length"`
}

type NumberFormat struct {
    Type    string `xml:"Type"`
    NDec    int    `xml:"nDec"`
    UseThou int    `xml:"UseThou"`
    Fmt     string `xml:"Fmt"`
    Dec     string `xml:"Dec"`
    Thou    string `xml:"Thou"`
}
```

Validation:

- Fail if `RecordByteSize <= 0` while `NoOfRecords > 0`.
- Fail if a field has a negative bit offset or bit width.
- Fail if bit ranges overlap unexpectedly.
- Allow `BitWidth == 0`; this means the field has only one symbol.
- Warn or fail on unsupported `Compression` header values if present.
- Treat encrypted QVDs as unsupported in the initial version.

## Field Metadata Model

Create a normalized internal column model:

```go
type Column struct {
    Name        string
    QlikType    QlikType
    BitOffset   int
    BitWidth    int
    Bias        int64
    SymbolCount int64
    Offset      int64
    Length      int64
    Selected    bool
}
```

Qlik type enum:

```go
type QlikType int

const (
    QlikUnknown QlikType = iota
    QlikASCII
    QlikDate
    QlikTimestamp
    QlikTime
    QlikInteger
    QlikReal
    QlikFix
    QlikMoney
)
```

Selection:

- Initial version can convert all fields.
- Add `--columns a,b,c` early because it is useful for large QVDs.
- When columns are skipped, still advance over skipped symbol-table bytes using header `Length`.
- Record bit ranges still include skipped columns, so record decoding needs all column bit metadata.

## Symbol Table Decoding

QVD symbol tables are byte-stuffed and appear after the XML header. The Java reader handles these symbol tags:

- `0x00`: empty/null-like symbol payload.
- `0x01`: 4-byte little-endian integer.
- `0x02`: 8-byte little-endian IEEE 754 double.
- `0x04`: UTF-8 string terminated by `0x00`.
- `0x05`: 4-byte integer followed by UTF-8 string, unless field type is `INTEGER`.
- `0x06`: 8-byte double followed by UTF-8 string, unless field type is `REAL`.

Represent symbols as typed values:

```go
type SymbolKind uint8

const (
    SymbolNull SymbolKind = iota
    SymbolString
    SymbolInt
    SymbolFloat
    SymbolDualIntString
    SymbolDualFloatString
)

type Symbol struct {
    Kind  SymbolKind
    Int   int64
    Float float64
    Text  string
}
```

Important behavior:

- Preserve dual values as dual values initially; do not discard either side during symbol decoding.
- Use `int32` when reading `0x01` and `0x05`, then widen to `int64` internally.
- Use `math.Float64frombits(binary.LittleEndian.Uint64(...))` for doubles.
- Read UTF-8 strings until the terminating null byte.
- Fail on unknown symbol tag by default.
- Include a future compatibility option `--unknown-symbol=error|string-empty|raw-hex` only if real files require it.

Memory strategy:

- Initial implementation: load selected columns' symbols into memory.
- Store symbols per selected column as `[]Symbol`.
- For skipped columns, seek/read past `Length`.
- Later optimization: if a column has very large strings and is selected, consider a disk-backed symbol store or memory-mapped string arena.

## Column Type Profiling

After each selected column's symbols are read, compute a profile:

```go
type ColumnProfile struct {
    Nulls       int64
    Strings     int64
    Ints        int64
    Floats      int64
    DualInts    int64
    DualFloats  int64
    EmptyText   int64
    MaxTextLen  int
    MinInt      int64
    MaxInt      int64
    MinFloat    float64
    MaxFloat    float64
}
```

Derived helpers:

- `HasOnlyNulls()`
- `HasOnlyText()`
- `HasOnlyInts()`
- `HasOnlyFloats()`
- `HasOnlyNumeric()`
- `HasDuals()`
- `HasMixedScalarFamilies()`
- `CanPromoteIntToFloat()`
- `CanUseQlikDeclaredType()`

The profile is the point where mixed-type detection happens. No Parquet schema should be created before profiles are available.

## Mixed-Type Strategy

QVD columns can contain multiple symbol encodings. That does not always mean the logical column is invalid:

- `int + null` is fine.
- `float + null` is fine.
- `int + float` can be promoted to `float64`.
- `dual numeric + string` is a normal Qlik concept, but Parquet needs an explicit representation.
- `number + unrelated text` should not silently become numeric.

Add a required schema resolution policy with a conservative default.

CLI:

```sh
--mixed error|string|promote|dual-columns
```

Policy definitions:

### `--mixed error`

Default.

Behavior:

- Allow nulls with any single logical type.
- Allow declared Qlik date/time/timestamp numeric encodings if symbols are numeric or dual numeric.
- Allow `int + float` only if the declared Qlik type is numeric and `--numeric-promote` is enabled; otherwise fail.
- Fail on string mixed with numeric values unless the symbols are Qlik duals and a dual handling option is enabled.

Use this for stable ETL pipelines where unexpected schema drift should stop the job.

### `--mixed string`

Behavior:

- Convert the entire mixed column to Parquet UTF-8 string.
- For dual values, use the display string if present, otherwise format the numeric value.
- For numeric values, format with deterministic, locale-independent formatting.
- Preserve nulls as nulls.

Use this for maximum compatibility.

### `--mixed promote`

Behavior:

- Promote `int + float` to `float64`.
- Preserve pure text as string.
- For `number + text`, fail unless `--mixed-string-fallback` is also set.
- For dual values:
  - if declared type is date/time/timestamp/numeric, use numeric side;
  - otherwise use text side.

Use this when numeric stability is more important than retaining display strings.

### `--mixed dual-columns`

Behavior:

- For Qlik dual columns, create two Parquet columns:
  - original name: typed numeric/date/time/timestamp value
  - `${name}__text`: display text
- For non-dual mixed columns, behave like `error` unless combined with `--mixed-string-fallback`.

Use this when both Qlik numeric semantics and display strings matter.

Additional useful flags:

```sh
--numeric-promote=true|false
--dual text|numeric|columns
--mixed-string-fallback
--schema-report path.json
```

Recommended defaults:

```text
--mixed=error
--numeric-promote=true
--dual=numeric
--mixed-string-fallback=false
```

Default rationale:

- Fail on dangerous mixed columns.
- Preserve common numeric `int + float` cases.
- Treat Qlik duals as numeric by default because Qlik stores dates/times and formatted numerics as numeric values with display text.
- Require explicit user intent before converting a mixed column to string.

## Resolved Schema Model

After profiling, create a resolved schema independent of QVD internals:

```go
type ResolvedColumn struct {
    SourceIndex int
    Name        string
    ArrowType   arrow.DataType
    Nullable    bool
    Strategy    ValueStrategy
    ExtraTextColumn bool
}

type ValueStrategy int

const (
    StrategyNull ValueStrategy = iota
    StrategyString
    StrategyInt64
    StrategyFloat64
    StrategyDate32
    StrategyTimestampMillis
    StrategyTimeMillis
    StrategyDecimal
    StrategyDualNumeric
    StrategyDualText
)
```

Schema resolution rules:

- `QlikInteger` with int symbols: Arrow `int64`.
- `QlikReal` with numeric symbols: Arrow `float64`.
- `QlikFix` and `QlikMoney` with numeric symbols: Arrow decimal, not `float64`.
- `QlikDate`: Arrow `date32`.
- `QlikTimestamp`: Arrow `timestamp[ms]` initially.
- `QlikTime`: Arrow `time32[ms]` or `duration[ms]`; choose based on Arrow Go support and downstream compatibility.
- `QlikASCII` or pure text: Arrow `utf8`.
- all-null columns: Arrow `utf8` nullable by default unless a schema override says otherwise.

Decimal handling:

- Exact decimal preservation is mandatory for v1.
- `QlikMoney` and `QlikFix` must resolve to Parquet decimal types by default.
- Use Arrow decimal128 unless a real fixture proves decimal256 is required.
- Infer decimal scale from `NumberFormat.NDec`.
- Infer precision from the profiled symbols after scaling, including sign and integer digits.
- If precision exceeds decimal128 capacity, fail with a clear error unless a future decimal256 implementation is available.
- Do not silently write `MONEY` or `FIX` as `float64`.
- If a `MONEY` or `FIX` symbol is encoded as a binary double, convert it to a scaled integer using the declared scale and verify exactness after scaling.
- Exactness check:
  - compute `scaled = value * 10^scale`,
  - round only if the difference from the nearest integer is within a strict decimal conversion tolerance,
  - convert back and verify it matches the declared-scale decimal representation,
  - fail the column if any symbol cannot be represented exactly at the declared scale.
- The decimal conversion tolerance is only for binary floating-point representation noise when no display string is available; it must not allow values with more decimal places than the declared scale.
- Prefer parsing the display string side of dual values for `MONEY`/`FIX` when it is present and matches the declared decimal separator, because the string can preserve decimal intent better than the binary floating side.
- The schema/profile report must show decimal precision, scale, and whether values came from numeric payloads, dual display strings, or both.

Decimal conversion model:

```go
type DecimalSpec struct {
    Precision int32
    Scale     int32
}

type DecimalValue struct {
    Spec   DecimalSpec
    Scaled big.Int
}
```

Implementation rules:

- Store decimal working values as scaled integers.
- Use Arrow decimal builders only after the column's final precision and scale are resolved.
- Decimal min, max, and sum quality metrics must use scaled integer arithmetic.
- Decimal Parquet validation must compare exact scaled values, not floating-point approximations.
- If `NumberFormat.NDec` is missing for `MONEY` or `FIX`, infer scale from display strings if available.
- If neither `NDec` nor display strings provide a reliable scale, fail with a schema/type policy error and require a schema override.
- Schema overrides must support explicit decimal precision and scale:

```json
{
  "columns": {
    "Amount": { "type": "decimal", "precision": 18, "scale": 4 }
  }
}
```

Decimal CLI options:

```text
--decimal-source auto      Decimal extraction: auto|text|numeric
--decimal-strict true      Fail if exact decimal conversion cannot be proven
```

Defaults:

```text
--decimal-source=auto
--decimal-strict=true
```

Schema overrides:

- Add `--schema path.json` after the base converter works.
- Overrides should allow type pinning per column.
- Overrides should be validated against actual symbols before writing.

## Qlik Date/Time Conversion

Qlik date/time values use a serial day representation compatible with spreadsheet-style dates, where `25569` maps to Unix epoch day.

Implement:

```go
func qlikDaysToDate32(v float64) int32
func qlikDaysToTimestampMillis(v float64, loc *time.Location) int64
func qlikFractionToTimeMillis(v float64) int32
```

Decisions:

- Default timezone: local timezone is compatible with the Java behavior.
- CLI flag: `--timezone Europe/Berlin|UTC|Local`.
- For reproducible ETL, recommend `--timezone UTC`.
- Document the default clearly.

Date conversion:

- For `DATE`, use whole days after subtracting `25569`.
- For integer date symbols, accept integer days.

Timestamp conversion:

- Convert fractional days to milliseconds.
- Preserve nulls.
- Decide whether to round or truncate; match Java's `Math.round` initially.

Time conversion:

- Interpret value as fraction of one day.
- Store milliseconds since midnight if using `time32[ms]`.

## Bit-Stuffed Record Decoding

Records are fixed-size byte slices of length `RecordByteSize`.

Each selected field references a symbol ID encoded at:

- `BitOffset`
- `BitWidth`
- plus `Bias`

Implementation:

```go
func DecodeRecord(raw []byte, columns []Column, out []int64) error
```

Rules:

- Decode little-endian bits.
- If `BitWidth == 0`, symbol ID is `0`.
- Otherwise decode the unsigned bit range and add `Bias`.
- If the resulting symbol ID is negative, the value is null.
- If symbol ID is out of range for the column, fail with row number, column name, and symbol ID.

Optimization:

- Do not use the Java-style `findIndex` scan per bit in the final implementation.
- Decode each column independently from its `BitOffset` and `BitWidth`.
- Use a helper that extracts up to 64 bits from the record:

```go
func readBitsLE(raw []byte, bitOffset int, bitWidth int) uint64
```

This is simpler and faster:

- Compute starting byte.
- Load up to 9 bytes into a `uint64`/`uint128`-like path as needed.
- Shift by `bitOffset % 8`.
- Mask `(1 << bitWidth) - 1`.

Need to handle `bitWidth == 64` carefully to avoid shifting by 64.

## Parallel Record Decoding

Parallel record decoding is a v1 requirement.

QVD records are fixed-width after the symbol-table section. Once the symbol tables have been read, the converter knows:

- the absolute byte offset where records begin,
- `RecordByteSize`,
- `NoOfRecords`,
- all bit offsets and widths,
- all selected columns' symbol tables.

That makes the record area suitable for range-based parallel decoding.

CLI:

```text
--workers 0               Decode workers, 0 means runtime.NumCPU()
```

Default:

- `--workers=0`

Default rationale:

- Parquet consumers should not rely on physical row order.
- Writing chunks as workers finish avoids reorder buffers and keeps throughput higher.
- Quality gates are order-independent.

Architecture:

```go
type DecodeChunk struct {
    Index     int64
    StartRow  int64
    RowCount  int
    ByteOffset int64
}

type DecodeResult struct {
    Chunk     DecodeChunk
    Record    arrow.Record
    Metrics   QualityMetrics
    Err       error
}
```

Flow:

1. Main goroutine creates contiguous `DecodeChunk` work items.
2. Worker goroutines use `os.File.ReadAt` to read `RowCount * RecordByteSize` bytes.
3. Each worker owns its Arrow builders; builders are never shared across goroutines.
4. Each worker decodes records, resolves symbols, converts values, and emits one Arrow record.
5. Each worker also emits chunk-local quality metrics.
6. A single writer goroutine receives completed `DecodeResult` values and writes Arrow records to the Parquet writer.
7. Metrics are merged after each chunk is accepted by the writer.

Writer concurrency:

- Treat the Parquet writer as single-owner and not goroutine-safe.
- Parallelize decode and Arrow record construction, not the actual writes to one Parquet file.
- If writing becomes the bottleneck, tune compression and batch size before adding multiple output files.

Ordering:

- Default unordered mode writes chunks as they complete.
- The first implementation should support unordered mode only.
- A future `--preserve-row-order` option could be implemented by buffering completed chunks in a small map keyed by `Chunk.Index` and writing only when the next expected chunk is available.

Failure behavior:

- On the first worker error, cancel the context.
- Stop issuing new chunks.
- Drain in-flight results.
- Close and remove the temporary Parquet output.
- Return an input/decode or output/write error with row range and column context.

Quality metrics in parallel:

- `basic` and `numeric` metrics are naturally mergeable across chunks.
- For `full` mode, use order-independent row and column fingerprints by default.
- Ordered hashing should not be required for normal validation because physical row order is not part of the converter contract.

## Batch Conversion

Use batch-oriented writing:

```sh
--batch-rows 65536
```

Default:

- `65536` rows per Arrow record initially.
- Tune with benchmarks.

Flow:

1. Create chunk work sized by `--batch-rows`.
2. In each worker, create Arrow record builders from the resolved schema.
3. For each row in the worker's chunk:
   - read `RecordByteSize` bytes,
   - decode all needed symbol IDs,
   - for each resolved output column, append null or converted value.
4. When the chunk is complete, create an Arrow record and send it to the writer goroutine.
5. Release Arrow record memory after writing.
6. Reset builders or create a fresh builder for the next chunk.

Memory target:

- Peak memory should be roughly:
  - selected symbol tables,
  - `workers * batchRows` Arrow builder memory,
  - Parquet writer buffers,
  - small decode scratch buffers.

Avoid:

- `[]map[string]any`
- `[]struct` materialization for all rows
- formatting numeric values to strings unless the policy requires it

## Parquet Writer

Create Arrow schema from `ResolvedColumn`.

Writer properties:

- Compression from CLI.
- Dictionary encoding enabled for strings by default.
- Reasonable row group size via batch rows.
- Statistics enabled by default.
- Store Arrow schema metadata if supported.

Output behavior:

- Refuse to overwrite existing output unless `--force` is set.
- Write to temporary output path first, then rename on success:

```text
output.parquet.tmp-<pid>
```

- Remove temp file on failure.

This prevents incomplete Parquet files from replacing previous good outputs.

## Quality Gate

Add an optional conversion quality gate that validates the written Parquet file against metrics collected while reading the QVD.

The goal is not to prove every cell is identical by default. For large files, full cell-by-cell validation can double the runtime and memory pressure. The default quality gate should provide high-signal checks that catch common conversion defects:

- row loss or duplication,
- null handling mistakes,
- numeric conversion errors,
- date/time conversion mistakes,
- column omission or schema mismatch,
- accidental stringification or type drift.

CLI:

```text
--quality-gate none        Validation mode: none|basic|numeric|full
--quality-report path.json Write quality metrics and comparison result
--quality-tolerance 1e-9   Relative tolerance for floating-point aggregate comparisons
--quality-abs-tolerance 0  Absolute tolerance for floating-point aggregate comparisons
```

Recommended default:

```text
--quality-gate=none
```

Recommended production setting:

```text
--quality-gate=numeric --quality-report out.quality.json
```

Quality gate modes:

### `none`

No post-write validation.

Use this for maximum throughput or when validation is handled by a downstream orchestrator.

### `basic`

Validate structural properties only:

- Parquet file can be opened.
- Parquet row count equals QVD `NoOfRecords`.
- Parquet column count and names match the resolved output schema.
- Parquet physical/logical types match the resolved schema.
- Per-column null counts match the QVD-side null counts.

This should be cheap and safe to run frequently.

### `numeric`

Run all `basic` checks plus numeric/date/time aggregates.

For every output column with a numeric-compatible resolved type, collect source-side metrics during conversion:

- non-null count,
- null count,
- sum,
- min,
- max,
- optional sum of squares for stronger drift detection.

Numeric-compatible types:

- `int64`
- `float64`
- decimal128 / decimal256
- `date32`
- `timestamp[ms]`
- `time32[ms]` or selected time representation

For date/time/timestamp columns, aggregate the stored physical value:

- `date32`: days since Unix epoch,
- `timestamp[ms]`: milliseconds since Unix epoch,
- `time32[ms]`: milliseconds since midnight.

After the Parquet file is written and closed, reopen it with Arrow Go and recompute the same metrics from the Parquet data. Compare source metrics with Parquet metrics.

Floating-point comparison:

- Use exact comparison for integer, decimal, date/time counts, nulls, min, max, and decimal sums.
- Use tolerance comparison for floating-point sum and sum of squares.
- Apply both relative and absolute tolerances:

```go
abs(a-b) <= absTolerance || abs(a-b) <= relTolerance * max(abs(a), abs(b), 1)
```

This mode catches most practical conversion problems while keeping validation cheaper than full cell comparison.

### `full`

Run all `numeric` checks plus deterministic order-independent row-level and column-level fingerprinting.

During conversion:

- Convert each emitted value into a canonical binary representation after applying the resolved schema strategy.
- Compute deterministic `sha256` digests for canonical column values and complete canonical rows.
- Merge digests into order-independent per-column and whole-file fingerprints.
- Include null markers explicitly.
- Include output column names, output column types, and column order in the row-hash preamble.

After writing:

- Read Parquet back.
- Canonicalize values using the same resolved schema.
- Recompute the same order-independent fingerprints.
- Compare per-column fingerprints and whole-file fingerprints.

Hash recommendation:

- Use `hash/maphash` only for internal non-stable hashes; do not use it for persisted reports.
- For reportable deterministic hashes, use `crypto/sha256`.
- Use a multiset-style fingerprint rather than an ordered stream hash:
  - count rows,
  - XOR row digests,
  - add row digests modulo `2^256`,
  - optionally add a second modular sum of `digest * digest` for stronger duplicate sensitivity.
- Keep hashing streaming; do not materialize all rows.

This mode is more expensive but useful for release testing, migration testing, and validating tricky mixed-type policy changes.

### Metrics Model

Internal model:

```go
type QualityMode int

const (
    QualityNone QualityMode = iota
    QualityBasic
    QualityNumeric
    QualityFull
)

type ColumnQualityMetrics struct {
    Name      string
    Type      string
    Rows      int64
    Nulls     int64
    NonNulls  int64
    Sum       decimalOrFloat
    Min       scalarValue
    Max       scalarValue
    SumSquares decimalOrFloat
    Hash      []byte
}

type QualityReport struct {
    Input       string
    Output      string
    Mode        string
    Passed      bool
    RowsSource  int64
    RowsParquet int64
    Columns     []ColumnQualityComparison
    Errors      []string
}
```

Implementation notes:

- Source-side metrics must be collected after mixed-type strategy conversion, not from raw QVD symbols. This validates what should have been written to Parquet.
- Parquet-side metrics must be computed by reading the final Parquet file, not by reusing in-memory Arrow batches from the writer.
- Quality gate errors should fail the CLI with a dedicated exit code.
- If the output is written to a temp path and then renamed, run the quality gate after the final rename or read from the temp file before rename and rename only after validation. Prefer validating the temp file before final rename so failed validation does not leave a final-looking output.

Report example:

```json
{
  "mode": "numeric",
  "passed": false,
  "rowsSource": 1000000,
  "rowsParquet": 1000000,
  "columns": [
    {
      "name": "Amount",
      "type": "decimal(18,2)",
      "source": { "nulls": 0, "sum": "123456.78", "min": "-10.50", "max": "999.00" },
      "parquet": { "nulls": 0, "sum": "123456.77", "min": "-10.50", "max": "999.00" },
      "passed": false,
      "errors": ["sum differs by 0.01"]
    }
  ]
}
```

## CLI Design

Initial command:

```sh
qvd2parquet [options] input.qvd output.parquet
```

Options:

```text
--columns name1,name2       Convert only selected columns
--mixed error               Mixed-type strategy: error|string|promote|dual-columns
--dual numeric              Dual strategy: numeric|text|columns
--numeric-promote true      Allow int+float promotion to float64
--mixed-string-fallback     Convert otherwise-invalid mixed columns to string
--decimal-source auto       Decimal extraction: auto|text|numeric
--decimal-strict true       Fail if exact decimal conversion cannot be proven
--compression zstd          Parquet compression: zstd|snappy|gzip|uncompressed
--batch-rows 65536          Rows per Arrow batch
--workers 0                 Decode workers, 0 means runtime.NumCPU()
--timezone Local            Local|UTC|IANA timezone name
--schema path.json          Optional explicit schema override
--schema-report path.json   Write inferred schema/profile report
--quality-gate none         Validation mode: none|basic|numeric|full
--quality-report path.json  Write post-conversion quality report
--quality-tolerance 1e-9    Relative tolerance for floating-point quality checks
--quality-abs-tolerance 0   Absolute tolerance for floating-point quality checks
--progress 1000000          Log every N rows, 0 disables progress
--force                     Overwrite existing output
--strict                    Enable strict validation defaults
```

Exit codes:

- `0`: success.
- `1`: CLI usage error.
- `2`: unsupported QVD feature.
- `3`: schema/type policy error.
- `4`: input read/decode error.
- `5`: output/write error.
- `6`: quality gate failure.

Progress output:

- Print to stderr, not stdout.
- Include row count, elapsed time, rows/sec, and output bytes if available.

## Schema/Profile Report

When `--schema-report report.json` is set, emit a machine-readable report:

```json
{
  "input": "file.qvd",
  "tableName": "Table",
  "rows": 123456,
  "recordByteSize": 42,
  "columns": [
    {
      "name": "Amount",
      "qlikType": "MONEY",
      "symbols": 1200,
      "profile": {
        "nulls": 1,
        "ints": 0,
        "floats": 1199,
        "strings": 0,
        "dualFloats": 0
      },
      "resolvedType": "float64",
      "strategy": "StrategyFloat64"
    }
  ]
}
```

This is important for debugging mixed-type failures and documenting schema decisions.

## Error Handling

Errors must include enough context to fix data or policy settings.

Examples:

```text
mixed type column "CustomerID": symbols contain 182331 integers and 12 strings; use --mixed=string to write this column as UTF-8
```

```text
row 1182739 column "Status": decoded symbol id 9123, but symbol table has 9123 entries
```

```text
unsupported encrypted QVD file: encrypted QVDs are not supported by qvd2parquet
```

Use wrapped Go errors internally:

```go
return fmt.Errorf("read symbols for column %q: %w", col.Name, err)
```

## Testing Strategy

### Unit Tests

`internal/qvd/header_test.go`

- Header XML unmarshalling.
- Missing optional fields.
- Bad numeric fields.

`internal/qvd/symbols_test.go`

- Decode each symbol tag.
- Decode integer little-endian.
- Decode double little-endian.
- Decode zero-terminated UTF-8.
- Preserve dual int/string and dual float/string.
- Fail on unknown tag.

`internal/qvd/bitpack_test.go`

- Decode single field.
- Decode multiple fields crossing byte boundaries.
- Decode `BitWidth == 0`.
- Decode bias.
- Decode null via negative biased symbol ID.
- Compare against known Java output for sample records.

`internal/convert/policy_test.go`

- Pure string column resolves to string.
- Pure integer resolves to int64.
- Integer plus float promotes when allowed.
- Integer plus string fails under `--mixed=error`.
- Integer plus string resolves to string under `--mixed=string`.
- Dual column resolves according to `--dual`.
- Date/timestamp/time types resolve correctly.
- `MONEY` and `FIX` resolve to decimal, not float.
- Decimal precision and scale are inferred from `NumberFormat.NDec` and profiled values.
- Decimal schema overrides are validated against actual values.

`internal/convert/batch_test.go`

- Append nulls.
- Append typed symbols.
- Append dual text column when configured.
- Append decimal values through Arrow decimal builders with exact scaled integers.
- Fail on out-of-range symbol ID.

`internal/convert/decimal_test.go`

- Parse decimal values from dual display text using the declared decimal separator.
- Convert binary numeric payloads to scaled integers only when exact at the declared scale.
- Fail when a `MONEY`/`FIX` value cannot be represented exactly.
- Infer precision from positive and negative scaled values.
- Fail when precision exceeds the supported decimal type.
- Preserve decimal min, max, and sum with scaled integer arithmetic.

`internal/convert/parallel_test.go`

- Decode multiple chunks concurrently from a synthetic fixed-width record area.
- Verify unordered chunk completion still writes all rows.
- Verify worker-local Arrow builders are independent.
- Verify worker errors cancel the conversion and remove temp output.
- Verify source-side quality metrics merge correctly across chunks.

`internal/convert/quality_test.go`

- Collect row counts and null counts from converted source values.
- Collect exact integer/date/time min, max, and sum metrics.
- Collect exact decimal min, max, and sum metrics.
- Collect floating-point metrics with deterministic tolerance comparison.
- Hash canonicalized null, string, integer, float, date, timestamp, and time values.
- Hash canonicalized decimal values using precision, scale, and scaled integer bytes.
- Report quality gate failures with column names and metric names.

### Integration Tests

Use real small QVD files in `testdata/` if licensing allows committing them.

For each fixture:

1. Convert QVD to Parquet.
2. Read Parquet back.
3. Compare row count.
4. Compare schema.
5. Compare selected cell values.
6. Run `--quality-gate=basic`.
7. Run `--quality-gate=numeric` for fixtures with numeric/date/time columns.
8. Run `--quality-gate=full` on at least one small fixture.

If real QVD fixtures cannot be committed:

- Keep local fixture instructions in `testdata/README.md`.
- Add tests that run only when `QVD2PARQUET_TESTDATA` points to a fixture directory.

### Cross-Checks Against Java Reader

For initial validation:

- Use the Java reader to produce CSV for sample QVD files.
- Use Go converter to produce Parquet.
- Read Parquet with DuckDB or Arrow and compare values.

Comparison caveats:

- Java reader formats dates/times as locale-specific strings.
- Go converter should preserve typed dates/timestamps, so comparisons need normalized values.

### Parquet Validation

Use at least one external reader:

- DuckDB
- PyArrow
- parquet-tools, if available

Validation examples:

```sh
duckdb -c "select count(*) from read_parquet('out.parquet')"
duckdb -c "describe select * from read_parquet('out.parquet')"
```

### Quality Gate Validation

Test the built-in quality gate independently from external tools:

- A correct conversion passes `basic`, `numeric`, and `full` modes.
- A deliberately truncated Parquet file fails before metrics comparison.
- A mismatched row count fails `basic`.
- A changed null count fails `basic`.
- A changed integer sum fails `numeric`.
- A changed decimal sum fails `numeric` exactly, with no floating-point tolerance.
- A changed floating-point sum fails `numeric` when outside tolerance.
- A changed string value fails `full`.
- `--quality-report` is written on both success and failure.

## Benchmarking Strategy

Benchmarks should separate parser and writer costs.

Measurements:

- QVD read/decode rows/sec.
- Parquet write rows/sec.
- Total conversion rows/sec.
- Peak RSS memory.
- Output Parquet size.
- Compression tradeoffs.
- Scaling by worker count: `--workers=1`, `2`, `4`, `8`, and default.
- Quality gate overhead by mode.

Datasets:

- Small fixture: correctness.
- Medium fixture: developer iteration.
- Large fixture: performance and memory.
- High-cardinality string fixture.
- Wide table fixture.
- Mixed-type/dual-heavy fixture.

Useful benchmark commands:

```sh
/usr/bin/time -l qvd2parquet input.qvd output.parquet
```

On Linux:

```sh
/usr/bin/time -v qvd2parquet input.qvd output.parquet
```

Performance target for first version:

- Faster than Java QVD-to-CSV plus a CSV-to-Parquet conversion.
- Stable memory use bounded by symbol tables plus configured batch size.

Optimization passes:

1. Replace per-bit decoding with per-field bit extraction.
2. Reuse record buffers.
3. Avoid string formatting for numeric/date paths.
4. Precompute column conversion functions.
5. Tune `--workers` and `--batch-rows` together.
6. Tune Arrow batch size.
7. Tune Parquet compression and dictionary settings.

## Implementation Milestones

### Milestone 1: Project Skeleton

Deliverables:

- `go.mod`
- `cmd/qvd2parquet/main.go`
- basic CLI parsing
- package layout
- placeholder README

Acceptance:

- `go test ./...` passes.
- `qvd2parquet --help` shows options.

### Milestone 2: Header Reader

Deliverables:

- read XML header until `0x00`
- unmarshal required metadata
- normalize columns
- header validation

Acceptance:

- Can print table name, row count, record byte size, and column list for a QVD.

### Milestone 3: Symbol Decoder

Deliverables:

- decode symbol tables into `[]Symbol`
- skip unselected columns
- profile columns after symbol decoding

Acceptance:

- Can print symbol profiles for each column.
- Unit tests cover all known symbol tags.

### Milestone 4: Schema Resolution and Mixed-Type Policy

Deliverables:

- policy implementation
- Arrow schema creation
- exact decimal schema resolution for `MONEY` and `FIX`
- decimal precision/scale inference
- decimal schema override validation
- schema/profile report JSON

Acceptance:

- Mixed columns fail or resolve according to CLI policy.
- `MONEY` and `FIX` never resolve to `float64` by default.
- Decimal values that cannot be represented exactly fail under `--decimal-strict=true`.
- `--schema-report` explains all decisions.

### Milestone 5: Record Decoder

Deliverables:

- fast `readBitsLE`
- full record decoder
- symbol ID validation
- null handling
- parallel chunk scheduler
- worker-local Arrow batch construction
- unordered chunk delivery to the writer

Acceptance:

- Decoded records match the Java reader for sample files.
- Bit boundary unit tests pass.
- `--workers=1` and default worker count produce equivalent unordered Parquet contents under `--quality-gate=full`.

### Milestone 6: Parquet Writer

Deliverables:

- Arrow builders
- Parquet file writer
- Parquet decimal output using Arrow decimal builders
- batch flushing
- temp output plus atomic rename
- source-side quality metric collection for row count, null count, and numeric aggregates

Acceptance:

- Converts a small QVD to readable Parquet.
- DuckDB/PyArrow can read row count and schema.
- Decimal columns are readable as Parquet decimal with expected precision and scale.
- `--quality-gate=basic` and `--quality-gate=numeric` pass for small numeric fixtures.

### Milestone 7: Large File Readiness

Deliverables:

- progress logging
- memory measurement notes
- batch size tuning
- selected column conversion
- worker count tuning
- quality gate performance measurement on medium and large files

Acceptance:

- Converts a large QVD without unbounded memory growth.
- User can reduce memory via `--columns`, `--batch-rows`, and `--workers`.
- `--quality-gate=numeric` has documented runtime overhead.
- Parallel decoding has documented scaling results and default worker guidance.

### Milestone 8: Hardening

Deliverables:

- better error messages
- more fixtures
- benchmark results
- full row-level hashing quality gate
- quality report examples
- README usage examples
- release build instructions

Acceptance:

- Clear documented behavior for mixed types, dual values, dates, timestamps, and unsupported QVD features.
- Clear documented behavior for `none`, `basic`, `numeric`, and `full` quality gate modes.

## Known Unsupported Features for v1

- Encrypted QVD files.
- Writing QVD files.
- Nested Parquet output.
- Full locale-specific display formatting.
- Disk-backed symbol table cache.
- Preserving QVD physical row order in Parquet output.

## Future Enhancements

- Disk-backed or mmap-backed symbol storage for very large string dictionaries.
- Multiple output files / partitioned output.
- Optional physical row-order preservation.
- Multi-file parallel output for partitioned exports.
- Direct Arrow IPC output.
- JSON schema override generator.
- Column rename support.
- Include original QVD XML header in Parquet key-value metadata.

## First Implementation Notes

Start with correctness over maximum speed, but avoid architectural choices that force full-row materialization.

The most important early design choice is storing QVD symbols as typed values and resolving the Parquet schema only after symbol profiling. That makes mixed-type behavior explicit and prevents the converter from silently producing unstable Parquet schemas.
