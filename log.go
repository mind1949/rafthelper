package rafthelper

import (
	"encoding/json"

	"github.com/mind1949/raft"
	bolt "go.etcd.io/bbolt"
)

var (
	bucketRaftLog = []byte("raft.log.bucket")
)

func NewLog(db *bolt.DB) (*Log, error) {
	_, err := createBucket(db, bucketRaftLog)
	if err != nil {
		return nil, err
	}

	return &Log{
		db: db,

		marshal:   json.Marshal,
		unmarshal: json.Unmarshal,
	}, nil
}

var _ raft.Log = (*Log)(nil)

type Log struct {
	db *bolt.DB

	marshal   func(interface{}) ([]byte, error)
	unmarshal func([]byte, interface{}) error
}

// Get 获取 raft log 中索引为 index 的 log entry term
func (l *Log) Get(index int) (term int, err error) {
	err = l.db.View(func(tx *bolt.Tx) error {
		key := itob(index)
		body := l.getBucket(tx).Get(key)
		if body == nil {
			return nil
		}

		var entry raft.LogEntry
		err := l.unmarshal(body, &entry)
		if err != nil {
			return err
		}

		term = entry.Term
		return nil
	})
	return term, err
}

// Match 是否有匹配上 term 与 index 的 log entry
func (l *Log) Match(index, term int) (bool, error) {
	target, err := l.Get(index)
	if err != nil {
		return false, err
	}

	return term == target, nil
}

// Last 返回最后一个 log entry 的 term 与 index
// 若无, 则返回 0 , 0
func (l *Log) Last() (index, term int, err error) {
	err = l.db.View(func(tx *bolt.Tx) error {
		key, value := l.getBucket(tx).Cursor().Last()
		if value == nil {
			return nil
		}

		index = btoi(key)
		var entry raft.LogEntry
		err := l.unmarshal(value, &entry)
		if err != nil {
			return err
		}
		term = entry.Term
		return nil
	})
	return index, term, nil
}

// RangeGet 获取在 (i, j] 索引区间的 log entry
func (l *Log) RangeGet(i, j int) (entries []raft.LogEntry, err error) {
	err = l.db.View(func(tx *bolt.Tx) error {
		c := l.getBucket(tx).Cursor()
		for k, v := c.Seek(itob(i + 1)); k != nil; k, v = c.Next() {
			var entry raft.LogEntry
			err := l.unmarshal(v, &entry)
			if err != nil {
				return err
			}
			entries = append(entries, entry)
		}
		return nil
	})
	return entries, err
}

// PopAfter 删除索引 i 之后的所有 log entry
func (l *Log) PopAfter(i int) error {
	return l.db.Update(func(tx *bolt.Tx) error {
		bucket := l.getBucket(tx)
		last, _, err := l.Last()
		if err != nil {
			return err
		}
		for i <= last {
			_ = bucket.Delete(itob(last))
			last--
		}
		return nil
	})
}

// Append 追加 log entry
func (l *Log) Append(entries ...raft.LogEntry) error {
	return l.db.Update(func(tx *bolt.Tx) error {
		last, _, err := l.Last()
		if err != nil {
			return err
		}
		bucket := l.getBucket(tx)
		for i, entry := range entries {
			idx := last + 1 + i
			entry.Index = idx
			value, err := l.marshal(entry)
			if err != nil {
				return err
			}
			bucket.Put(itob(idx), value)
		}
		return nil
	})
}

func (l *Log) getBucket(tx *bolt.Tx) *bolt.Bucket {
	return getBucket(tx, bucketRaftLog)
}
