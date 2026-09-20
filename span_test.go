package spanmerge

import (
	"path/filepath"
	"testing"
)

func seg(lo, hi int64) Segment { return Segment{lo, hi} }

func reopen(t *testing.T, path string) *Merger {
	t.Helper()
	m, err := Open(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	return m
}

func eqSegs(t *testing.T, got, want []Segment) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("segment count: got %v, want %v", got, want)
	}
	for i := range got {
		if got[i] != want[i] {
			t.Fatalf("segment[%d]: got %v, want %v (full got=%v want=%v)",
				i, got[i], want[i], got, want)
		}
	}
}

// Adjacent ranges collapse into one maximal segment; a later overlapping
// range extends it without duplicating indices.
func TestMergeAdjacentAndOverlap(t *testing.T) {
	path := filepath.Join(t.TempDir(), "log")
	m := reopen(t, path)
	defer m.Close()

	done, err := m.Add(0, 5)
	if err != nil || len(done) != 0 {
		t.Fatalf("first add: done=%v err=%v", done, err)
	}
	if done, err := m.Add(5, 9); err != nil || len(done) != 0 {
		t.Fatalf("adjacent add: done=%v err=%v", done, err)
	}
	if _, err := m.Add(2, 4); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Add(7, 12); err != nil {
		t.Fatal(err)
	}
	out, err := m.Flush()
	if err != nil {
		t.Fatal(err)
	}
	eqSegs(t, out, []Segment{seg(0, 12)})
}

// A real hole seals the earlier segment immediately and starts a new one.
func TestHoleSealsSegment(t *testing.T) {
	path := filepath.Join(t.TempDir(), "log")
	m := reopen(t, path)
	defer m.Close()

	if _, err := m.Add(0, 5); err != nil {
		t.Fatal(err)
	}
	done, err := m.Add(6, 9)
	if err != nil {
		t.Fatal(err)
	}
	eqSegs(t, done, []Segment{seg(0, 5)})
	eqSegs(t, m.Committed(), []Segment{seg(0, 5)})

	out, err := m.Flush()
	if err != nil {
		t.Fatal(err)
	}
	eqSegs(t, out, []Segment{seg(6, 9)})
	eqSegs(t, m.Committed(), []Segment{seg(0, 5), seg(6, 9)})
}

// Re-delivering an identical committed segment emits nothing, even after reopen.
func TestDuplicateSegmentEmittedOnce(t *testing.T) {
	path := filepath.Join(t.TempDir(), "log")
	m := reopen(t, path)
	defer m.Close()

	if _, err := m.Add(0, 5); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Add(6, 9); err != nil { // seals [0,5)
		t.Fatal(err)
	}
	if _, err := m.Flush(); err != nil { // commit trailing open segment
		t.Fatal(err)
	}
	done, err := m.Add(0, 5)
	if err != nil {
		t.Fatal(err)
	}
	if len(done) != 0 {
		t.Fatalf("duplicate range emitted: %v", done)
	}
	eqSegs(t, m.Committed(), []Segment{seg(0, 5), seg(6, 9)})

	m.Close()
	m = reopen(t, path)
	defer m.Close()
	if done, err := m.Add(0, 5); err != nil || len(done) != 0 {
		t.Fatalf("duplicate after reopen: done=%v err=%v", done, err)
	}
	eqSegs(t, m.Committed(), []Segment{seg(0, 5), seg(6, 9)})
}

// Injected failure before any byte is written: the open segment is lost,
// previously intact segments survive reopening.
func TestFailBeforeCommitReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "log")
	m := reopen(t, path)
	defer m.Close()

	if _, err := m.Add(0, 5); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Add(6, 10); err != nil { // seals [0,5)
		t.Fatal(err)
	}

	m.InjectFail(FailBeforeCommit)
	if _, err := m.Flush(); err == nil {
		t.Fatal("expected injected failure")
	}
	m.Close()

	m = reopen(t, path)
	defer m.Close()
	eqSegs(t, m.Committed(), []Segment{seg(0, 5)})
}

// Injected torn write leaves a partial trailing frame; reopen truncates it
// and keeps earlier complete frames; the lost piece recommits cleanly once.
func TestTornWriteTruncatedOnReopen(t *testing.T) {
	for _, site := range []string{FailTornHeader, FailTornWrite} {
		t.Run(site, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "log")
			m := reopen(t, path)

			if _, err := m.Add(0, 5); err != nil {
				t.Fatal(err)
			}
			if _, err := m.Add(6, 10); err != nil { // seals [0,5)
				t.Fatal(err)
			}
			m.InjectFail(site)
			if _, err := m.Flush(); err == nil {
				t.Fatal("expected injected failure")
			}
			m.Close()

			m = reopen(t, path)
			eqSegs(t, m.Committed(), []Segment{seg(0, 5)})

			if _, err := m.Add(6, 10); err != nil {
				t.Fatal(err)
			}
			out, err := m.Flush()
			if err != nil {
				t.Fatal(err)
			}
			eqSegs(t, out, []Segment{seg(6, 10)})
			m.Close()

			m = reopen(t, path)
			defer m.Close()
			eqSegs(t, m.Committed(), []Segment{seg(0, 5), seg(6, 10)})
		})
	}
}

// Completed segments come out sorted by Lo even with out-of-order input.
func TestOutOfOrderSortedByStart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "log")
	m := reopen(t, path)
	defer m.Close()

	if _, err := m.Add(20, 25); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Add(0, 5); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Add(5, 10); err != nil { // adjacent -> [0,10)
		t.Fatal(err)
	}
	done, err := m.Add(30, 35) // seals both earlier open segments
	if err != nil {
		t.Fatal(err)
	}
	eqSegs(t, done, []Segment{seg(0, 10), seg(20, 25)})

	out, err := m.Flush()
	if err != nil {
		t.Fatal(err)
	}
	eqSegs(t, out, []Segment{seg(30, 35)})
}

// Empty/invalid ranges are ignored and never commit anything.
func TestEmptyRangeIgnored(t *testing.T) {
	path := filepath.Join(t.TempDir(), "log")
	m := reopen(t, path)
	defer m.Close()

	if done, err := m.Add(5, 5); err != nil || len(done) != 0 {
		t.Fatalf("empty range: done=%v err=%v", done, err)
	}
	if done, err := m.Add(9, 3); err != nil || len(done) != 0 {
		t.Fatalf("inverted range: done=%v err=%v", done, err)
	}
	if out, err := m.Flush(); err != nil || len(out) != 0 {
		t.Fatalf("flush after empties: out=%v err=%v", out, err)
	}
}
