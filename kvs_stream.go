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

// joinrToBytes concatenates multiple byte slices into a single byte slice
func joinToBytes(parts ...[]byte) []byte {
	// Calculate total length to pre-allocate the result slice
	totalLen := 0
	for _, part := range parts {
		totalLen += len(part)
	}

	// Allocate a single slice for the result
	result := make([]byte, totalLen)

	// Copy each part into the result slice
	currentPos := 0
	for _, part := range parts {
		copy(result[currentPos:], part)
		currentPos += len(part)
	}

	return result
}

// cut removes all the data associated with a specific feed or key range.
// It's just a placeholder, as RocksDB doesn’t natively support this, but can be implemented.
func (r *RocksDB) Cut(feed etf.ErlTerm) error {
	// Implementation to delete a specific range from RocksDB
	iter := r.db.NewIterator(r.ro)
	defer iter.Close()
	start, err := etf.EncodeErlTerm(feed, true)
	if err != nil {
		return err
	}

	end, err := etf.EncodeErlTerm(feed, true)
	if err != nil {
		return err
	}
	batch := grocksdb.NewWriteBatch()
	defer batch.Destroy()

	for iter.Seek(start); iter.Valid(); iter.Next() {
		key := iter.Key()
		if bytes.Compare(key.Data(), end) >= 0 {
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

// take retrieves a specific number of key-value pairs starting from a given key.
func (r *RocksDB) Take(startKey etf.ErlTerm, num int) (map[string]string, error) {
	iter := r.db.NewIterator(r.ro)
	defer iter.Close()

	result := make(map[string]string)
	start, err := etf.EncodeErlTerm(startKey, true)
	if err != nil {
		return nil, err
	}
	iter.Seek(start)
	count := 0

	for ; iter.Valid() && count < num; iter.Next() {
		key := iter.Key()
		value := iter.Value()

		result[string(key.Data())] = string(value.Data())

		key.Free()
		value.Free()

		count++
	}

	if err := iter.Err(); err != nil {
		return nil, err
	}

	return result, nil
}

// drop removes a specific number of key-value pairs starting from a given key.
func (r *RocksDB) Drop(startKey etf.ErlTerm, num int) error {
	iter := r.db.NewIterator(r.ro)
	defer iter.Close()
	start, err := etf.EncodeErlTerm(startKey, true)
	if err != nil {
		return err
	}
	iter.Seek(start)
	count := 0

	for ; iter.Valid() && count < num; iter.Next() {
		key := iter.Key()
		r.db.Delete(r.wo, key.Data()) // Delete the key
		key.Free()
		count++
	}

	if err := iter.Err(); err != nil {
		return err
	}

	return nil
}

// top retrieves the first key-value pair in the database
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

	return nil, nil, UnknownError
}

// bot retrieves the last key-value pair in the database
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

	return nil, nil, UnknownError
}

// next retrieves the next key-value pair after the provided startKey
func (r *RocksDB) Next(startKey []byte) (string, string, error) {
	iter := r.db.NewIterator(r.ro)
	defer iter.Close()

	iter.Seek(startKey)
	if iter.Valid() {
		iter.Next()
		if iter.Valid() {
			key := iter.Key()
			value := iter.Value()

			defer key.Free()
			defer value.Free()

			return string(key.Data()), string(value.Data()), nil
		}
	}

	if err := iter.Err(); err != nil {
		return "", "", err
	}

	return "", "", UnknownError
}

// prev retrieves the previous key-value pair before the provided startKey
func (r *RocksDB) Prev(startKey []byte) (string, string, error) {
	iter := r.db.NewIterator(r.ro)
	defer iter.Close()

	iter.Seek(startKey)
	if iter.Valid() {
		iter.Prev()
		if iter.Valid() {
			key := iter.Key()
			value := iter.Value()

			defer key.Free()
			defer value.Free()

			return string(key.Data()), string(value.Data()), nil
		}
	}

	if err := iter.Err(); err != nil {
		return "", "", err
	}

	return "", "", UnknownError
}

// loadReader loads a specific reader from the database using the given key
func (r *RocksDB) LoadReader(id etf.ErlTerm) (etf.ErlTerm, error) {
	idb, err := etf.EncodeErlTerm(id, true)
	if err != nil {
		return nil, err
	}
	value, err := r.db.Get(r.ro, idb)
	if err != nil {
		return "", err
	}
	defer value.Free()

	if value.Size() == 0 {
		return "", nil
	}

	v, err := etf.DecodeErlTerm(value.Data())
	if err != nil {
		return nil, err
	}
	return v, nil
}

// saveReader saves a reader to the database with the specified key and value
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

// remove deletes a record from the database using the given key
func (r *RocksDB) Remove(key etf.ErlTerm) error {
	keyb, err := etf.EncodeErlTerm(key, true)
	if err != nil {
		return err
	}
	return r.db.Delete(r.wo, keyb)
}

// append adds a record to the database if it doesn't already exist, otherwise returns the existing key
func (r *RocksDB) Append(rec etf.ErlTerm, feed etf.ErlTerm) (etf.ErlTerm, error) {
	recb, err := etf.EncodeErlTerm(rec, true)
	if err != nil {
		return nil, err
	}
	existingValue, err := r.db.Get(r.ro, recb)
	if err != nil {
		return nil, err
	}
	defer existingValue.Free()

	if existingValue.Size() > 0 {
		// Record already exists, return existing ID
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
