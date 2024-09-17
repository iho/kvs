package kvs

import (
	"bytes"
	"errors"

	"github.com/iho/etf"
	"github.com/linxGnu/grocksdb"
)

var (
	UnknownError = errors.New("unknown error")
)

// RocksDB represents a RocksDB instance and associated options
type RocksDB struct {
	db *grocksdb.DB
	ro *grocksdb.ReadOptions
	wo *grocksdb.WriteOptions
}

func NewRocksDB(db *grocksdb.DB, ro *grocksdb.ReadOptions, wo *grocksdb.WriteOptions) *RocksDB {
	return &RocksDB{
		db: db,
		ro: ro,
		wo: wo,
	}
}

// Cut removes all the data associated with a specific feed or key range.
func (r *RocksDB) Cut(feed etf.ErlTerm) error {
	start, err := etf.EncodeErlTerm(feed, true)
	if err != nil {
		return err
	}

	// To define an end key, we can append a byte that is greater than any possible byte in the start key
	end := append(start, 0xFF)

	iter := r.db.NewIterator(r.ro)
	defer iter.Close()

	batch := grocksdb.NewWriteBatch()
	defer batch.Destroy()

	for iter.Seek(start); iter.Valid(); iter.Next() {
		key := iter.Key()
		if bytes.Compare(key.Data(), end) >= 0 {
			key.Free()
			break
		}
		batch.Delete(key.Data())
		key.Free()
	}

	if err := iter.Err(); err != nil {
		return err
	}

	return r.db.Write(r.wo, batch)
}

// Take retrieves a specific number of key-value pairs starting from a given key.
func (r *RocksDB) Take(startKey etf.ErlTerm, num int) (etf.Map, error) {
	iter := r.db.NewIterator(r.ro)
	defer iter.Close()

	result := etf.Map{}
	start, err := etf.EncodeErlTerm(startKey, true)
	if err != nil {
		return nil, err
	}

	iter.Seek(start)
	count := 0

	for ; iter.Valid() && count < num; iter.Next() {
		key := iter.Key()
		value := iter.Value()

		k, err := etf.DecodeErlTerm(key.Data())
		if err != nil {
			key.Free()
			value.Free()
			return nil, err
		}

		v, err := etf.DecodeErlTerm(value.Data())
		if err != nil {
			key.Free()
			value.Free()
			return nil, err
		}

		result = append(result, etf.MapElem{Key: k, Value: v})

		key.Free()
		value.Free()

		count++
	}

	if err := iter.Err(); err != nil {
		return nil, err
	}

	return result, nil
}

// Drop removes a specific number of key-value pairs starting from a given key.
func (r *RocksDB) Drop(startKey etf.ErlTerm, num int) error {
	iter := r.db.NewIterator(r.ro)
	defer iter.Close()

	start, err := etf.EncodeErlTerm(startKey, true)
	if err != nil {
		return err
	}

	batch := grocksdb.NewWriteBatch()
	defer batch.Destroy()

	iter.Seek(start)
	count := 0

	for ; iter.Valid() && count < num; iter.Next() {
		key := iter.Key()
		batch.Delete(key.Data())
		key.Free()
		count++
	}

	if err := iter.Err(); err != nil {
		return err
	}

	return r.db.Write(r.wo, batch)
}

// Top retrieves the first key-value pair in the database
func (r *RocksDB) Top() (etf.ErlTerm, etf.ErlTerm, error) {
	iter := r.db.NewIterator(r.ro)
	defer iter.Close()

	iter.SeekToFirst()
	if iter.Valid() {
		key := iter.Key()
		value := iter.Value()

		defer key.Free()
		defer value.Free()

		k, err := etf.DecodeErlTerm(key.Data())
		if err != nil {
			return nil, nil, err
		}
		v, err := etf.DecodeErlTerm(value.Data())
		if err != nil {
			return nil, nil, err
		}
		return k, v, nil
	}

	if err := iter.Err(); err != nil {
		return nil, nil, err
	}

	return nil, nil, errors.New("database is empty")
}

// Bot retrieves the last key-value pair in the database
func (r *RocksDB) Bot() (etf.ErlTerm, etf.ErlTerm, error) {
	iter := r.db.NewIterator(r.ro)
	defer iter.Close()

	iter.SeekToLast()
	if iter.Valid() {
		key := iter.Key()
		value := iter.Value()

		defer key.Free()
		defer value.Free()

		k, err := etf.DecodeErlTerm(key.Data())
		if err != nil {
			return nil, nil, err
		}
		v, err := etf.DecodeErlTerm(value.Data())
		if err != nil {
			return nil, nil, err
		}
		return k, v, nil
	}

	if err := iter.Err(); err != nil {
		return nil, nil, err
	}

	return nil, nil, errors.New("database is empty")
}

// Next retrieves the next key-value pair after the provided startKey
func (r *RocksDB) Next(startKey etf.ErlTerm) (etf.ErlTerm, etf.ErlTerm, error) {
	iter := r.db.NewIterator(r.ro)
	defer iter.Close()

	start, err := etf.EncodeErlTerm(startKey, true)
	if err != nil {
		return nil, nil, err
	}

	iter.Seek(start)
	if iter.Valid() {
		// If the startKey exists, move to the next key
		key := iter.Key()
		if bytes.Equal(key.Data(), start) {
			iter.Next()
			key.Free()
		} else {
			key.Free()
		}
	}

	if iter.Valid() {
		key := iter.Key()
		value := iter.Value()

		defer key.Free()
		defer value.Free()

		k, err := etf.DecodeErlTerm(key.Data())
		if err != nil {
			return nil, nil, err
		}
		v, err := etf.DecodeErlTerm(value.Data())
		if err != nil {
			return nil, nil, err
		}

		return k, v, nil
	}

	if err := iter.Err(); err != nil {
		return nil, nil, err
	}

	return nil, nil, errors.New("no next key")
}

// Prev retrieves the previous key-value pair before the provided startKey
func (r *RocksDB) Prev(startKey etf.ErlTerm) (etf.ErlTerm, etf.ErlTerm, error) {
	iter := r.db.NewIterator(r.ro)
	defer iter.Close()

	start, err := etf.EncodeErlTerm(startKey, true)
	if err != nil {
		return nil, nil, err
	}

	iter.Seek(start)
	if iter.Valid() {
		iter.Prev()
	} else {
		// If Seek failed, position at the last key less than startKey
		iter.SeekForPrev(start)
	}

	if iter.Valid() {
		key := iter.Key()
		value := iter.Value()

		defer key.Free()
		defer value.Free()

		k, err := etf.DecodeErlTerm(key.Data())
		if err != nil {
			return nil, nil, err
		}
		v, err := etf.DecodeErlTerm(value.Data())
		if err != nil {
			return nil, nil, err
		}

		return k, v, nil
	}

	if err := iter.Err(); err != nil {
		return nil, nil, err
	}

	return nil, nil, errors.New("no previous key")
}

// LoadReader loads a specific reader from the database using the given key
func (r *RocksDB) LoadReader(id etf.ErlTerm) (etf.ErlTerm, error) {
	idb, err := etf.EncodeErlTerm(id, true)
	if err != nil {
		return nil, err
	}
	value, err := r.db.GetBytes(r.ro, idb)
	if err != nil {
		return nil, err
	}

	if value == nil {
		return nil, errors.New("key not found")
	}

	v, err := etf.DecodeErlTerm(value)
	if err != nil {
		return nil, err
	}
	return v, nil
}

// SaveReader saves a reader to the database with the specified key and value
func (r *RocksDB) SaveReader(id etf.ErlTerm, data etf.ErlTerm) error {
	idb, err := etf.EncodeErlTerm(id, true)
	if err != nil {
		return err
	}
	dataErl, err := etf.EncodeErlTerm(data, true)
	if err != nil {
		return err
	}

	return r.db.Put(r.wo, idb, dataErl)
}

// Remove deletes a record from the database using the given key
func (r *RocksDB) Remove(key etf.ErlTerm) error {
	keyb, err := etf.EncodeErlTerm(key, true)
	if err != nil {
		return err
	}
	return r.db.Delete(r.wo, keyb)
}

// Append adds a record to the database if it doesn't already exist, otherwise returns the existing key
func (r *RocksDB) Append(rec etf.ErlTerm, feed etf.ErlTerm) (etf.ErlTerm, error) {
	recb, err := etf.EncodeErlTerm(rec, true)
	if err != nil {
		return nil, err
	}
	existingValue, err := r.db.GetBytes(r.ro, recb)
	if err != nil {
		return nil, err
	}

	if existingValue != nil {
		// Record already exists, return existing record
		return rec, nil
	}

	feedb, err := etf.EncodeErlTerm(feed, true)
	if err != nil {
		return nil, err
	}

	// Record does not exist, insert it
	if err := r.db.Put(r.wo, recb, feedb); err != nil {
		return nil, err
	}

	return rec, nil
}

// Close closes the database
func (r *RocksDB) Close() {
	r.db.Close()
}
