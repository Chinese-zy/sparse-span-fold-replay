package spanmerge

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"os"
)

// ErrInjected 是测试注入失败时返回的错误。
var ErrInjected = errors.New("injected failure")

// FailPoint 在写路径关键点被调用，返回非 nil 即模拟进程在该点被掐掉。
// op 取值："before_body"、"after_body"、"after_crc"、"after_sync"。
type FailPoint func(op string) error

const recordBodyLen = 4 + 8 + 8 // len + start + end
const recordLen = recordBodyLen + 4

// Store 是追加式 WAL。记录格式：[len][start][end][crc32]，crc 覆盖前三个字段。
// 重开时只回放校验通过的完整记录，末尾半截记录被截断丢弃。
// 同一段重复提交不会重复落盘也不会重复吐出。
type Store struct {
	f      *os.File
	fail   FailPoint
	merger *Merger
	seen   map[Span]struct{}
}

// OpenStore 打开（或创建）WAL，回放完整记录并截断半截尾巴。fail 可为 nil。
func OpenStore(path string, fail FailPoint) (*Store, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}
	st := &Store{f: f, fail: fail, merger: NewMerger(), seen: make(map[Span]struct{})}
	if err := st.replay(); err != nil {
		f.Close()
		return nil, err
	}
	return st, nil
}

// replay 顺序扫描，遇到不完整或校验失败的记录即截断，丢弃其后所有字节。
func (st *Store) replay() error {
	if _, err := st.f.Seek(0, io.SeekStart); err != nil {
		return err
	}
	var good int64
	for {
		var body [recordBodyLen]byte
		if _, err := io.ReadFull(st.f, body[:]); err != nil {
			break
		}
		if binary.BigEndian.Uint32(body[0:4]) != recordBodyLen {
			break
		}
		var crcBuf [4]byte
		if _, err := io.ReadFull(st.f, crcBuf[:]); err != nil {
			break
		}
		if crc32.ChecksumIEEE(body[:]) != binary.BigEndian.Uint32(crcBuf[:]) {
			break
		}
		s := Span{
			Start: int64(binary.BigEndian.Uint64(body[4:12])),
			End:   int64(binary.BigEndian.Uint64(body[12:20])),
		}.normalize()
		if _, dup := st.seen[s]; !dup {
			st.seen[s] = struct{}{}
			st.merger.Add(s)
		}
		good += recordLen
	}
	if err := st.f.Truncate(good); err != nil {
		return err
	}
	_, err := st.f.Seek(good, io.SeekStart)
	return err
}

// Write 提交一个区间。重复提交同一段是幂等空操作。
// 写顺序：body -> crc -> sync，失败点可注入在任意两步之间；
// 任何一步失败都等价于进程被掐掉：记录不完整，重开时被丢弃。
func (st *Store) Write(s Span) error {
	s = s.normalize()
	if _, dup := st.seen[s]; dup {
		return nil
	}
	var rec [recordLen]byte
	binary.BigEndian.PutUint32(rec[0:4], recordBodyLen)
	binary.BigEndian.PutUint64(rec[4:12], uint64(s.Start))
	binary.BigEndian.PutUint64(rec[12:20], uint64(s.End))
	binary.BigEndian.PutUint32(rec[20:24], crc32.ChecksumIEEE(rec[:recordBodyLen]))

	if err := st.checkFail("before_body"); err != nil {
		return err
	}
	if _, err := st.f.Write(rec[:recordBodyLen]); err != nil {
		return err
	}
	if err := st.checkFail("after_body"); err != nil {
		return err
	}
	if _, err := st.f.Write(rec[recordBodyLen:]); err != nil {
		return err
	}
	if err := st.checkFail("after_crc"); err != nil {
		return err
	}
	if err := st.f.Sync(); err != nil {
		return err
	}
	if err := st.checkFail("after_sync"); err != nil {
		return err
	}
	st.seen[s] = struct{}{}
	st.merger.Add(s)
	return nil
}

func (st *Store) checkFail(op string) error {
	if st.fail != nil {
		return st.fail(op)
	}
	return nil
}

// Spans 吐出当前已收完整的段，按起点升序。
func (st *Store) Spans() []Span { return st.merger.Spans() }

func (st *Store) Close() error { return st.f.Close() }
