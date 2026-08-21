// Command genfixture writes a synthetic QVD file for benchmarking and manual
// testing. It is a development helper, not part of the qvd2parquet CLI.
//
//	go run ./internal/qvdtest/cmd/genfixture out.qvd [rows]
package main

import (
	"fmt"
	"os"
	"strconv"

	"github.com/ralforion/qvd2parquet/internal/qvd"
	"github.com/ralforion/qvd2parquet/internal/qvdtest"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: genfixture out.qvd [rows]")
		os.Exit(2)
	}
	rows := 200000
	if len(os.Args) > 2 {
		n, err := strconv.Atoi(os.Args[2])
		if err != nil || n < 0 {
			fmt.Fprintf(os.Stderr, "genfixture: bad row count %q\n", os.Args[2])
			os.Exit(2)
		}
		rows = n
	}
	ids := make([]int, rows)
	names := make([]int, rows)
	amounts := make([]int, rows)
	days := make([]int, rows)
	ratios := make([]int, rows)
	stamps := make([]int, rows)
	times := make([]int, rows)
	for i := 0; i < rows; i++ {
		ids[i] = i % 1000
		names[i] = i % 7
		amounts[i] = i % 5
		days[i] = i % 365
		if i%13 == 0 {
			ratios[i] = -1
		} else {
			ratios[i] = i % 11
		}
		stamps[i] = i % 97
		times[i] = i % 24
	}
	idSyms := make([]qvd.Symbol, 1000)
	for i := range idSyms {
		idSyms[i] = qvdtest.Int(int64(i * 3))
	}
	daySyms := make([]qvd.Symbol, 365)
	for i := range daySyms {
		daySyms[i] = qvdtest.Int(int64(45000 + i))
	}
	ratioSyms := make([]qvd.Symbol, 11)
	for i := range ratioSyms {
		ratioSyms[i] = qvdtest.Float(float64(i) * 1.5)
	}
	stampSyms := make([]qvd.Symbol, 97)
	for i := range stampSyms {
		stampSyms[i] = qvdtest.Float(45000 + float64(i)/97.0)
	}
	timeSyms := make([]qvd.Symbol, 24)
	for i := range timeSyms {
		timeSyms[i] = qvdtest.Float(float64(i) / 24.0)
	}

	tbl := qvdtest.Table{Name: "BigSales", Fields: []qvdtest.Field{
		{Name: "Id", Type: "INTEGER", Symbols: idSyms, Rows: ids},
		{Name: "Name", Type: "ASCII", Rows: names, Symbols: []qvd.Symbol{
			qvdtest.Str("alpha"), qvdtest.Str("beta"), qvdtest.Str("gamma"),
			qvdtest.Str("delta"), qvdtest.Str("epsilon"), qvdtest.Str("zeta"), qvdtest.Str(""),
		}},
		{Name: "Amount", Type: "MONEY", NDec: 2, Dec: ",", Thou: ".", Rows: amounts, Symbols: []qvd.Symbol{
			qvdtest.DualFloat(1234.56, "1.234,56"),
			qvdtest.DualFloat(-10.5, "-10,50"),
			qvdtest.DualFloat(0, "0,00"),
			qvdtest.DualFloat(99999.99, "99.999,99"),
			qvdtest.DualFloat(7.25, "7,25"),
		}},
		{Name: "Day", Type: "DATE", Symbols: daySyms, Rows: days},
		{Name: "Ratio", Type: "REAL", Symbols: ratioSyms, Rows: ratios},
		{Name: "Seen", Type: "TIMESTAMP", Symbols: stampSyms, Rows: stamps},
		{Name: "Clock", Type: "TIME", Symbols: timeSyms, Rows: times},
	}}

	n, err := qvdtest.Build(os.Args[1], tbl)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Printf("wrote %s with %d rows\n", os.Args[1], n)
}
