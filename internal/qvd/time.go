package qvd

import (
	"fmt"
	"math"
	"strings"
	"time"
)

// QlikEpochOffset is the Qlik/spreadsheet serial day number of 1970-01-01.
const QlikEpochOffset = 25569

const millisPerDay = 86400000

// ParseLocation resolves the --timezone flag value.
func ParseLocation(name string) (*time.Location, error) {
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "", "local":
		return time.Local, nil
	case "utc":
		return time.UTC, nil
	}
	loc, err := time.LoadLocation(name)
	if err != nil {
		return nil, fmt.Errorf("unknown timezone %q: %w", name, err)
	}
	return loc, nil
}

// QlikDaysToDate32 converts a Qlik serial day number to days since the Unix
// epoch. The fractional part is discarded, so the result is timezone
// independent.
func QlikDaysToDate32(v float64) (int32, bool) {
	d := math.Floor(v) - QlikEpochOffset
	if math.IsNaN(d) || d < math.MinInt32 || d > math.MaxInt32 {
		return 0, false
	}
	return int32(d), true
}

// QlikDaysToTimestampMillis converts a Qlik serial timestamp to milliseconds
// since the Unix epoch, interpreting the serial value as wall-clock time in
// loc. Rounding matches the Java reference reader's Math.round.
func QlikDaysToTimestampMillis(v float64, loc *time.Location) (int64, bool) {
	wall := (v - QlikEpochOffset) * millisPerDay
	if math.IsNaN(wall) || math.IsInf(wall, 0) || math.Abs(wall) > 1e18 {
		return 0, false
	}
	ms := int64(math.Round(wall))
	if loc == time.UTC {
		return ms, true
	}
	// Reinterpret the wall clock in loc rather than in UTC.
	t := time.UnixMilli(ms).UTC()
	local := time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second(), t.Nanosecond(), loc)
	return local.UnixMilli(), true
}

// QlikFractionToTimeMillis converts a Qlik time value (a fraction of one day)
// to milliseconds since midnight.
func QlikFractionToTimeMillis(v float64) (int32, bool) {
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return 0, false
	}
	_, frac := math.Modf(v)
	if frac < 0 {
		frac += 1
	}
	ms := int64(math.Round(frac * millisPerDay))
	// Rounding can push 23:59:59.9995 onto the next day.
	if ms >= millisPerDay {
		ms -= millisPerDay
	}
	return int32(ms), true
}
