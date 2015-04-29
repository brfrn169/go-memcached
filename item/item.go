package item

import (
	"github.com/brfrn169/go-memcached/util"
)

type Item struct {
	Key    []byte
	Value  []byte
	Flags  []byte
	CAS    uint64
	Expiry uint64
	next   *Item
}

func newItem(key []byte, value []byte, flags []byte, expiry uint32) *Item {
	var e uint64
	if expiry > 0 {
		e = util.GetNowMillis() + uint64(expiry)
	}

	return &Item{
		Key:    key,
		Value:  value,
		Flags:  flags,
		CAS:    uint64(1),
		Expiry: e,
	}
}
