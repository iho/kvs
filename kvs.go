package kvs

import "github.com/iho/etf"

// put/1 — put record using id as a key.
// get/2 — get record by key from table.
// delete/1 — delete record from table.
// index/3 — search records by field and its value.
// seq/2 — generate new sequence table id.
// count/1 — counts records in table.
// dir/0 — table list.

type KVS interface {
	Put(id etf.ErlTerm, data etf.ErlTerm) error
	Get(id etf.ErlTerm) (etf.ErlTerm, error)
	Delete(id etf.ErlTerm) error
	Index(field etf.ErlTerm, value etf.ErlTerm) ([]etf.ErlTerm, error)
	Seq() (etf.ErlTerm, error)
	Count() (int64, error)
	Dir() ([]etf.ErlTerm, error)
}
