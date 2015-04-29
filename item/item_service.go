package item

import (
    "sync"
    "hash/crc32"
    "bytes"
)

type ItemService struct {
    hashArray []*Item
    hashArraySize uint32
    lock sync.Mutex
}

func NewItemService() *ItemService {
    // TODO expire thread

    var hashArraySize uint32 = 1024 // TODO make it configurable
    return &ItemService{
        hashArraySize: hashArraySize,
        hashArray: make([]*Item, hashArraySize),
    }
}

func (s *ItemService) hashIndex(key []byte) uint32 {
    // TODO make it bitwise operation
    return crc32.ChecksumIEEE(key) % s.hashArraySize
}

func (s *ItemService) Set(key []byte, value []byte, flags []byte, expiry uint32) *Item {
    s.lock.Lock()
    defer s.lock.Unlock()

    index := s.hashIndex(key)
    if s.hashArray[index] == nil {
        s.hashArray[index] = newItem(key, value, flags, expiry)
        return s.hashArray[index]
    }
    for it := s.hashArray[index]; it != nil; it = it.next {
        if bytes.Equal(it.Key, key) {
            it.Value = value
            it.Flags = flags
            it.CAS += 1
            return it
        } else if it.next == nil {
            it.next = newItem(key, value, flags, expiry)
            return it.next
        }
    }

    return nil
}

func (s *ItemService) Get(key []byte) *Item {
    s.lock.Lock()
    defer s.lock.Unlock()

    index := s.hashIndex(key)

    for it := s.hashArray[index]; it != nil; it = it.next {
        if bytes.Equal(key, it.Key) {
            // TODO handle expiry
            return it
        }
    }

    return nil
}

