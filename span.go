// Package spanmerge merges a stream of sparse, half-open index ranges
// [Lo, Hi) into maximal contiguous segments and durably records the
// completed segments in a write-ahead log.
//
// Merging rules:
//
//   - Adjacent ranges ([0,5) and [5,9)) collapse into one segment [0,9).
//   - A real hole (e.g. [0,5) then [6,9)) can never be bridged: the
//     segment before the hole is committed and a new one is opened.
//   - Overlapping input contributes each index only once (set-union).
//   - Input that arrives later wins for the intersected part: when a new
//     range reaches into the open segment it extends it, while the
//     already covered part is absorbed rather than duplicated.
//
// Durability rules:
//
//   - Only fully completed segments are appended to the log. The segment
//     still being assembled lives in memory; if the process is killed
//     mid-way it is discarded on reopen.
//   - Each log frame carries a checksum and a length prefix. Torn or
//     incomplete trailing frames are truncated on open.
//   - Delivering the same segment again cannot be emitted twice: frames
//     already present in the log are skipped on recovery and on reopen.
package spanmerge

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"os"
	"sort"
)

// Segment is one maximal contiguous, half-open range of indices.
type Segment struct {
	Lo int64
	Hi int64
}

// Len returns the number of indices covered by the segment.
func (s Segment) Len() int64 { return s.Hi - s.Lo }

// frame layout: u32 bodyLen || magic(2) | version(1) | lo(8) | hi(8) | crc(4).
const (
	magic0   = 0x53
	magic1   = 0x4D // "SM"
	version1 = 1
	headerSz = 2 + 1 + 8 + 8 + 4
)

// Injectable failure-site names. They never kill the OS process; they
// simulate a crash by returning an error and leaving the file exactly as
// a real crash at that byte boundary would.
const (
	// FailBeforeCommit fires before any byte for the segment is written.
	FailBeforeCommit = "before_commit"
	// FailTornHeader writes the length prefix and a partial body, then fails.
	FailTornHeader = "torn_header"
	// FailTornWrite writes the length prefix and all but one body byte.
	FailTornWrite = "torn_write"
)

// Merger accumulates sparse ranges and durably commits completed segments.
type Merger struct {
	path    string
	f       *os.File
	closed  []Segment // intact segments already present in the log
	pending []Segment // in-memory segments still being assembled
	fail    string    // one-shot armed failure site
}

// Open opens (creating if needed) the log at path and recovers every
// complete, intact segment previously committed, truncating torn tails.
func Open(path string) (*Merger, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}
	committed, validLen, err := scanFrames(f)
	if err != nil {
		f.Close()
		return nil, err
	}
	if err := f.Truncate(validLen); err != nil {
		f.Close()
		return nil, err
	}
	if _, err := f.Seek(0, io.SeekEnd); err != nil {
		f.Close()
		return nil, err
	}
	return &Merger{path: path, f: f, closed: committed}, nil
}

// InjectFail arms a single failure at site; it fires on the next commit.
func (m *Merger) InjectFail(site string) { m.fail = site }

// Add folds the half-open range [lo, hi) into the merger and returns the
// segments that complete because of this call, ordered by start index.
// Empty or invalid ranges are ignored.
//
// A segment completes when the new range starts strictly beyond the open
// segment's end (a real hole); use Flush to finish the trailing segment.
func (m *Merger) Add(lo, hi int64) ([]Segment, error) {
	if hi <= lo {
		return nil, nil
	}
	in := Segment{lo, hi}

	// Trim anything already covered durably; committed indices win.
	in = subtractClosed(in, m.closed)
	if in.Hi <= in.Lo {
		return nil, nil
	}

	m.pending = coalesce(m.pending)

	var finished []Segment

	// Locate position relative to existing open segments.
	placed := false
	for i := range m.pending {
		p := &m.pending[i]
		switch {
		case in.Hi < p.Lo || in.Lo > p.Hi:
			// disjoint with this piece
		case in.Lo >= p.Lo && in.Hi <= p.Hi:
			placed = true // entirely contained: nothing new
		default:
			// Overlap (or adjacency): union; later writer extends it.
			if in.Lo < p.Lo {
				p.Lo = in.Lo
			}
			if in.Hi > p.Hi {
				p.Hi = in.Hi
			}
			placed = true
		}
		if placed {
			break
		}
	}

	if !placed {
		// New range is disjoint from every open segment. Open pieces that
		// lie entirely before it are sealed because a hole now follows.
		var keep []Segment
		for _, p := range m.pending {
			if p.Hi < in.Lo {
				finished = append(finished, p)
			} else {
				keep = append(keep, p)
			}
		}
		m.pending = append(keep, in)
	}

	m.pending = coalesce(m.pending)

	if len(finished) > 0 {
		sort.Slice(finished, func(a, b int) bool { return finished[a].Lo < finished[b].Lo })
		if err := m.appendFrames(finished); err != nil {
			return nil, err
		}
		m.closed = coalesce(append(m.closed, finished...))
	}
	return finished, nil
}

// Flush commits every still-open segment, ordered by start index, and
// returns them. After a crash only previously committed segments survive;
// uncommitted work is intentionally discarded on reopen.
func (m *Merger) Flush() ([]Segment, error) {
	m.pending = coalesce(m.pending)
	if len(m.pending) == 0 {
		return nil, nil
	}
	out := make([]Segment, len(m.pending))
	copy(out, m.pending)
	if err := m.appendFrames(out); err != nil {
		return nil, err
	}
	m.closed = coalesce(append(m.closed, out...))
	m.pending = nil
	return out, nil
}

// Committed returns a copy of every intact logged segment, ordered by Lo.
func (m *Merger) Committed() []Segment {
	out := make([]Segment, len(m.closed))
	copy(out, m.closed)
	return out
}

// Close releases the file. Uncommitted work is not force-flushed, since it
// is meant to be discardable on simulated crash.
func (m *Merger) Close() error { return m.f.Close() }

// appendFrames writes segments one frame at a time, honoring a one-shot
// injected failure that leaves a realistic torn trail on disk.
func (m *Merger) appendFrames(segs []Segment) error {
	if m.fail != "" {
		site := m.fail
		m.fail = ""
		if site == FailBeforeCommit {
			return &FailError{Site: site}
		}
		if err := m.writeTorn(segs[0], site); err != nil {
			return err
		}
		segs = segs[1:]
	}
	for _, s := range segs {
		if err := m.writeFrame(s); err != nil {
			return err
		}
	}
	return m.f.Sync()
}

// writeTorn simulates a crash: length prefix plus a truncated body.
func (m *Merger) writeTorn(s Segment, site string) error {
	body, err := encodeFrame(s)
	if err != nil {
		return err
	}
	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], uint32(len(body)))
	if _, err := m.f.Write(lenBuf[:]); err != nil {
		return err
	}
	frag := len(body) / 2
	if site == FailTornWrite {
		frag = len(body) - 1
	}
	if frag < 1 {
		frag = 1
	}
	if _, err := m.f.Write(body[:frag]); err != nil {
		return err
	}
	_ = m.f.Sync()
	return &FailError{Site: site}
}

func (m *Merger) writeFrame(s Segment) error {
	body, err := encodeFrame(s)
	if err != nil {
		return err
	}
	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], uint32(len(body)))
	if _, err := m.f.Write(lenBuf[:]); err != nil {
		return err
	}
	if _, err := m.f.Write(body); err != nil {
		return err
	}
	return nil
}

// FailError reports that an injected failure point fired.
type FailError struct{ Site string }

func (e *FailError) Error() string {
	return "spanmerge: injected failure at " + e.Site
}

func encodeFrame(s Segment) ([]byte, error) {
	if s.Hi < s.Lo {
		return nil, errors.New("spanmerge: negative-length segment")
	}
	b := make([]byte, headerSz)
	b[0] = magic0
	b[1] = magic1
	b[2] = version1
	binary.BigEndian.PutUint64(b[3:11], uint64(s.Lo))
	binary.BigEndian.PutUint64(b[11:19], uint64(s.Hi))
	binary.BigEndian.PutUint32(b[19:23], crc32.ChecksumIEEE(b[:19]))
	return b, nil
}

// scanFrames reads intact frames and returns them plus the valid byte
// length; any torn or corrupt tail is excluded.
func scanFrames(f *os.File) ([]Segment, int64, error) {
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return nil, 0, err
	}
	data, err := io.ReadAll(f)
	if err != nil {
		return nil, 0, err
	}
	var segs []Segment
	pos := 0
	for pos+4 <= len(data) {
		total := int(binary.BigEndian.Uint32(data[pos : pos+4]))
		end := pos + 4 + total
		if total <= 0 || total > 1<<20 || end > len(data) {
			break // truncated length/body: stop at the last good frame
		}
		s, ok := decodeFrame(data[pos+4 : end])
		if !ok {
			break // corrupt frame: preserve only the valid prefix
		}
		segs = append(segs, s)
		pos = end
	}
	return coalesce(segs), int64(pos), nil
}

func decodeFrame(b []byte) (Segment, bool) {
	if len(b) != headerSz || b[0] != magic0 || b[1] != magic1 || b[2] != version1 {
		return Segment{}, false
	}
	if binary.BigEndian.Uint32(b[19:23]) != crc32.ChecksumIEEE(b[:19]) {
		return Segment{}, false
	}
	lo := int64(binary.BigEndian.Uint64(b[3:11]))
	hi := int64(binary.BigEndian.Uint64(b[11:19]))
	if hi < lo {
		return Segment{}, false
	}
	return Segment{lo, hi}, true
}

// coalesce sorts segments and unions overlapping or touching pieces.
func coalesce(in []Segment) []Segment {
	if len(in) < 2 {
		return in
	}
	sort.Slice(in, func(a, b int) bool { return in[a].Lo < in[b].Lo })
	out := in[:1]
	for i := 1; i < len(in); i++ {
		last := &out[len(out)-1]
		cur := in[i]
		if cur.Lo <= last.Hi { // overlap or adjacency
			if cur.Hi > last.Hi {
				last.Hi = cur.Hi
			}
			continue
		}
		out = append(out, cur)
	}
	return out
}

// subtractClosed removes committed coverage from a single connected range.
// It clips from the covered edges; a fully covered range collapses empty.
func subtractClosed(s Segment, closed []Segment) Segment {
	for _, c := range closed {
		if c.Hi <= s.Lo || c.Lo >= s.Hi {
			continue
		}
		if c.Lo <= s.Lo {
			if c.Hi >= s.Hi {
				return Segment{}
			}
			s.Lo = c.Hi
		} else if c.Hi >= s.Hi {
			s.Hi = c.Lo
		}
	}
	return s
}
