package kvs

import "github.com/iho/etf"

// RocksDB is a key-value store implementation using RocksDB as the backend.

type KVSRocksDB struct {
	db *RocksDB
}

func NewKVSRocksDB(db *RocksDB) *KVSRocksDB {
	return &KVSRocksDB{
		db: db,
	}
}

func (r *KVSRocksDB) Put(id etf.ErlTerm, data etf.ErlTerm) error {
	return r.db.SaveReader(id, data)
}

func (r *KVSRocksDB) Get(id etf.ErlTerm) (etf.ErlTerm, error) {
	return r.db.LoadReader(id)
}

func (r *KVSRocksDB) Delete(id etf.ErlTerm) error {
	return r.db.Remove(id)
}

// I have no idea how to implement this function
func (r *KVSRocksDB) Index(field etf.ErlTerm, value etf.ErlTerm) ([]etf.ErlTerm, error) {
	return nil, nil
}

// I have no idea how to implement this function
func (r *KVSRocksDB) Seq() (etf.ErlTerm, error) {
	return nil, nil
}

func (r *KVSRocksDB) Count() (int64, error) {
	return r.db.Count()
}

func (r *KVSRocksDB) Dir() ([]etf.ErlTerm, error) {
	return r.db.All(etf.Atom(""))
}
