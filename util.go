package rafthelper

import (
	"bytes"
	"encoding/binary"

	bolt "go.etcd.io/bbolt"
)

func createBucket(db *bolt.DB, bucketName []byte) (*bolt.Bucket, error) {
	tx, err := db.Begin(true)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	bucket, err := tx.CreateBucketIfNotExists(bucketName)
	if err != nil {
		return nil, err
	}
	err = tx.Commit()
	if err != nil {
		return nil, err
	}

	return bucket, nil
}

func getBucket(tx *bolt.Tx, name []byte) *bolt.Bucket {
	return tx.Bucket(name)
}

// itob returns an 8-byte big endian representation of v.
func itob(v int) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(v))
	return b
}

func btoi(b []byte) int {
	return int(binary.BigEndian.Uint64(b))
}

func compare(i, j int) int {
	return bytes.Compare(itob(i), itob(j))
}
