package server

import (
	"encoding/binary"
	"fmt"
	"net"

	"github.com/brfrn169/go-memcached/item"
	"github.com/brfrn169/go-memcached/util"
)

type RequestProcessor struct {
	conn        *net.TCPConn
	itemService *item.ItemService
	wCh         chan writeRequest
}

type writeRequest struct {
	opcode byte
	status uint16
	opaque []byte
	cas    uint64
	flags  []byte
	key    []byte
	value  []byte
}

func NewRequestProcessor(conn *net.TCPConn, itemService *item.ItemService) *RequestProcessor {
	return &RequestProcessor{
		conn:        conn,
		itemService: itemService,
		wCh:         make(chan writeRequest),
	}
}

func (rp *RequestProcessor) Start() {
	go rp.processRead()
	go rp.processWrite()
}

func (rp *RequestProcessor) processRead() {
	defer rp.conn.Close()
	defer close(rp.wCh)

	for {
		rh, err := ReadReqHeader(rp.conn)
		if err != nil {
			fmt.Printf("Read Header Error [%v]: %v\n", rp.conn.RemoteAddr(), err)
			return
		}

		fmt.Printf("Read opaque [%v]: %v\n", rp.conn.RemoteAddr(), rh.Opaque)

		if rh.Opcode == OpGet {
			key, err := util.TcpRead(rp.conn, uint32(rh.KeyLength))
			if err != nil {
				fmt.Printf("Read Error [%v]: %v\n", rp.conn.RemoteAddr(), err)
				return
			}

			// TODO limit number of process
			go rp.get(rh, key)
		} else if rh.Opcode == OpSet {
			flags, err := util.TcpRead(rp.conn, uint32(4))
			if err != nil {
				fmt.Printf("Read Error [%v]: %v\n", rp.conn.RemoteAddr(), err)
				return
			}

			expiry, err := util.TcpRead(rp.conn, uint32(4))
			if err != nil {
				fmt.Printf("Read Error [%v]: %v\n", rp.conn.RemoteAddr(), err)
				return
			}

			key, err := util.TcpRead(rp.conn, uint32(rh.KeyLength))
			if err != nil {
				fmt.Printf("Read Error [%v]: %v\n", rp.conn.RemoteAddr(), err)
				return
			}

			valueLen := rh.TotalBodyLength - uint32(rh.ExtrasLength) - uint32(rh.KeyLength)
			value, err := util.TcpRead(rp.conn, valueLen)
			if err != nil {
				fmt.Printf("Read Error [%v]: %v\n", rp.conn.RemoteAddr(), err)
				return
			}

			// TODO limit number of process
			go rp.set(rh, key, value, flags, binary.BigEndian.Uint32(expiry))
		}
	}

}

func (rp *RequestProcessor) processWrite() {
	defer rp.conn.Close()

	for {
		wr, more := <-rp.wCh

		if !more {
			return
		}

		err := WriteRes(rp.conn, wr.opcode, wr.status, wr.opaque, wr.cas, wr.flags, wr.key, wr.value)
		if err != nil {
			fmt.Printf("Write Error [%v]: %v\n", rp.conn.RemoteAddr(), err)
			return
		}

		//		fmt.Printf("write opaque [%v]: %v\n", rp.conn.RemoteAddr(), wr.opaque)
	}
}

func (rp *RequestProcessor) get(rh *ReqHeader, key []byte) {
	i := rp.itemService.Get(key)

	//    fmt.Printf("key:%v, value:%v, flags:%v, cas:%v\n", i.Key, i.Value, i.Flags, i.Cas)

	// TODO status
	rp.wCh <- writeRequest{
		opcode: OpGet,
		status: StatusNoError,
		opaque: rh.Opaque,
		cas:    0, /*i.CAS*/
		flags:  i.Flags,
		key:    nil,
		value:  i.Value,
	}
}

func (rp *RequestProcessor) set(rh *ReqHeader, key []byte, value []byte, flags []byte, expiry uint32) {
	// TODO cas

	//    fmt.Printf("key:%v, value:%v, flags:%v, expiry:%v\n", key, value, flags, expiry)

	//	i := rp.itemService.Set(key, value, flags, expiry)
	rp.itemService.Set(key, value, flags, expiry)

	// TODO status
	rp.wCh <- writeRequest{
		opcode: OpSet,
		status: StatusNoError,
		opaque: rh.Opaque,
		cas:    0, /*i.CAS*/
		flags:  nil,
		key:    nil,
		value:  nil,
	}
}
