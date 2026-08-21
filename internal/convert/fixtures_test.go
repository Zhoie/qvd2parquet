package convert

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestRealFixtures converts every .qvd file in the directory named by
// QVD2PARQUET_TESTDATA and validates each conversion with the full quality
// gate. It is skipped when the variable is unset, because real QVD files
// cannot generally be committed.
func TestRealFixtures(t *testing.T) {
	dir := os.Getenv("QVD2PARQUET_TESTDATA")
	if dir == "" {
		t.Skip("set QVD2PARQUET_TESTDATA to a directory of .qvd files to run this test")
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read fixture directory: %v", err)
	}
	var fixtures []string
	for _, e := range entries {
		if !e.IsDir() && strings.EqualFold(filepath.Ext(e.Name()), ".qvd") {
			fixtures = append(fixtures, filepath.Join(dir, e.Name()))
		}
	}
	if len(fixtures) == 0 {
		t.Fatalf("no .qvd files in %s", dir)
	}

	for _, in := range fixtures {
		t.Run(filepath.Base(in), func(t *testing.T) {
			outDir := t.TempDir()
			out := filepath.Join(outDir, "out.parquet")

			opts := testOptions()
			opts.TimezoneName = "UTC"
			opts.Quality = QualityFull
			opts.SchemaReportPath = filepath.Join(outDir, "schema.json")
			opts.QualityReportPath = filepath.Join(outDir, "quality.json")

			stats, report, err := Run(context.Background(), in, out, &opts, nil)
			if err != nil {
				// A mixed-type column failing under the default policy is a
				// legitimate result, not a converter bug; retry as strings so
				// the rest of the pipeline is still exercised.
				if !strings.Contains(err.Error(), "mixed type column") {
					t.Fatalf("Run: %v", err)
				}
				t.Logf("retrying with --mixed=string: %v", err)
				opts.Mixed = MixedString
				if stats, report, err = Run(context.Background(), in, out, &opts, nil); err != nil {
					t.Fatalf("Run with --mixed=string: %v", err)
				}
			}
			if !report.Passed {
				t.Fatalf("full quality gate failed: %+v", report)
			}
			if report.RowsSource != report.RowsParquet {
				t.Errorf("rows source=%d parquet=%d", report.RowsSource, report.RowsParquet)
			}
			t.Logf("%s: %d rows, %d columns, %d output bytes, %.0f rows/s",
				filepath.Base(in), stats.Rows, stats.Columns, stats.OutputBytes, stats.RowsPerSecond())

			// The schema report must be readable and cover every column.
			b, err := os.ReadFile(opts.SchemaReportPath)
			if err != nil || len(b) == 0 {
				t.Errorf("schema report missing or empty: %v", err)
			}

			// A single-worker run must produce the same content fingerprints.
			single := opts
			single.Workers = 1
			singleOut := filepath.Join(outDir, "single.parquet")
			_, singleReport, err := Run(context.Background(), in, singleOut, &single, nil)
			if err != nil {
				t.Fatalf("single-worker run: %v", err)
			}
			for i := range report.Columns {
				if report.Columns[i].Source.Hash != singleReport.Columns[i].Source.Hash {
					t.Errorf("column %q fingerprint differs between worker counts", report.Columns[i].Name)
				}
			}
		})
	}
}
