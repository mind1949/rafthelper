package rafthelper

import (
	"github.com/mind1949/raft"
	bolt "go.etcd.io/bbolt"
)

var (
	bucketRaftStore = []byte("raft.store.bucket")
)

func NewStore(db *bolt.DB) (*Store, error) {
	_, err := createBucket(db, bucketRaftStore)
	if err != nil {
		return nil, err
	}

	return &Store{
		db: db,
	}, nil
}

var _ raft.Store = (*Store)(nil)

type Store struct {
	db *bolt.DB
}

func (s *Store) Set(key []byte, val []byte) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		return s.getBucket(tx).Put(key, val)
	})
}

// Get returns the value for key, or an empty byte slice if key was not found.
func (s *Store) Get(key []byte) ([]byte, error) {
	var value []byte
	err := s.db.View(func(tx *bolt.Tx) error {
		v := s.getBucket(tx).Get(key)
		value = make([]byte, len(v))
		copy(value, v)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return value, nil
}

func (s *Store) SetInt(key []byte, i int) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		return s.getBucket(tx).Put(key, i2b(i))
	})
}

// GetInt returns the int value for key, or 0 if key was not found.
func (s *Store) GetInt(key []byte) (int, error) {
	var i int
	err := s.db.View(func(tx *bolt.Tx) error {
		value := s.getBucket(tx).Get(key)
		if value == nil {
			return nil
		}

		i = b2i(value)
		return nil
	})
	if err != nil {
		return 0, err
	}
	return i, nil
}

func (s *Store) getBucket(tx *bolt.Tx) *bolt.Bucket {
	return tx.Bucket(bucketRaftStore)
}
