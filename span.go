package spanmerge

import "sort"

// Span 表示稀疏下标区间 [Start, End]，闭区间。
type Span struct {
	Start int64
	End   int64
}

func (s Span) normalize() Span {
	if s.Start > s.End {
		s.Start, s.End = s.End, s.Start
	}
	return s
}

// Merger 维护一组已合并的稀疏区间。
// 相邻（含首尾相接）的段收成一段；中间有洞不并；
// 重叠只留一份；后写的范围覆盖先写的相交部分（并集语义）。
type Merger struct {
	spans []Span // 按 Start 升序且互不相邻
}

func NewMerger() *Merger { return &Merger{} }

// Add 并入一个区间。
func (m *Merger) Add(s Span) {
	s = s.normalize()
	lo := sort.Search(len(m.spans), func(i int) bool { return m.spans[i].End >= s.Start-1 })
	hi := sort.Search(len(m.spans), func(i int) bool { return m.spans[i].Start > s.End+1 })
	merged := s
	if lo < len(m.spans) && m.spans[lo].Start < merged.Start {
		merged.Start = m.spans[lo].Start
	}
	if hi > 0 && m.spans[hi-1].End > merged.End {
		merged.End = m.spans[hi-1].End
	}
	tail := append([]Span{merged}, m.spans[hi:]...)
	m.spans = append(m.spans[:lo], tail...)
}

// Spans 返回合并结果，按起点升序。
func (m *Merger) Spans() []Span {
	out := make([]Span, len(m.spans))
	copy(out, m.spans)
	return out
}
