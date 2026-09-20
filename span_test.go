package spanmerge

import (
	"path/filepath"
	"reflect"
	"testing"
)

func TestMergeAdjacentOverlapAndHoles(t *testing.T) {
	m := NewMerger()
	for _, s := range []Span{{10, 12}, {1, 3}, {4, 5}, {2, 3}, {8, 9}, {20, 22}, {24, 25}} {
		m.Add(s)
	}
	want := []Span{{1, 5}, {8, 12}, {20, 22}, {24, 25}}
	if got := m.Spans(); !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v want %v", got, want)
	}
}

func TestLaterWriteCoversEarlierIntersection(t *testing.T) {
	m := NewMerger()
	m.Add(Span{5, 10})
	m.Add(Span{8, 20}) // 后写盖住先写的 [8,10]，重叠只留一份
	want := []Span{{5, 20}}
	if got := m.Spans(); !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v want %v", got, want)
	}
}

func TestReversedSpanNormalized(t *testing.T) {
	m := NewMerger()
	m.Add(Span{10, 5})
	want := []Span{{5, 10}}
	if got := m.Spans(); !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v want %v", got, want)
	}
}

func reopen(t *testing.T, path string, fail FailPoint) *Store {
	t.Helper()
	st, err := OpenStore(path, fail)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	return st
}

func TestCrashMidRecordDropsPartial(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal")

	st := reopen(t, path, nil)
	if err := st.Write(Span{1, 3}); err != nil {
		t.Fatal(err)
	}
	st.Close()

	// 第二段在 after_body 崩溃：body 落盘但 crc 缺失，成为半截段。
	crashed := reopen(t, path, func(op string) error {
		if op == "after_body" {
			return ErrInjected
		}
		return nil
	})
	if err := crashed.Write(Span{4, 6}); err != ErrInjected {
		t.Fatalf("want ErrInjected, got %v", err)
	}
	crashed.Close()

	// 重开：半截段 [4,6] 必须被丢掉，只剩 [1,3]。
	again := reopen(t, path, nil)
	defer again.Close()
	want := []Span{{1, 3}}
	if got := again.Spans(); !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v want %v", got, want)
	}

	// 截断后还能继续写，新段正常生效。
	if err := again.Write(Span{4, 6}); err != nil {
		t.Fatal(err)
	}
	want = []Span{{1, 6}}
	if got := again.Spans(); !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v want %v", got, want)
	}
}

func TestCrashBeforeBodyLeavesNothing(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal")
	st := reopen(t, path, nil)
	st.Write(Span{10, 12})
	st.Close()

	crashed := reopen(t, path, func(op string) error {
		if op == "before_body" {
			return ErrInjected
		}
		return nil
	})
	crashed.Write(Span{20, 22})
	crashed.Close()

	again := reopen(t, path, nil)
	defer again.Close()
	want := []Span{{10, 12}}
	if got := again.Spans(); !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v want %v", got, want)
	}
}

func TestDuplicateSpanNotEmittedTwice(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal")
	st := reopen(t, path, nil)
	for i := 0; i < 3; i++ {
		if err := st.Write(Span{1, 5}); err != nil {
			t.Fatal(err)
		}
	}
	st.Write(Span{7, 8})
	want := []Span{{1, 5}, {7, 8}}
	if got := st.Spans(); !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v want %v", got, want)
	}
	st.Close()

	// 重开后重复段仍然只出现一次（WAL 里也只有一条）。
	again := reopen(t, path, nil)
	defer again.Close()
	if got := again.Spans(); !reflect.DeepEqual(got, want) {
		t.Fatalf("after reopen got %v want %v", got, want)
	}
}

func TestReplayMergesAcrossSessions(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal")
	st := reopen(t, path, nil)
	st.Write(Span{1, 2})
	st.Write(Span{10, 11})
	st.Close()

	again := reopen(t, path, nil)
	defer again.Close()
	again.Write(Span{3, 9}) // 跨会话把两段桥接起来
	want := []Span{{1, 11}}
	if got := again.Spans(); !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v want %v", got, want)
	}
}
