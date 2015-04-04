package server

import (
    "net"
    "encoding/binary"
    "errors"

    "github.com/brfrn169/go-memcached/util"
)

const headerSize = 24

// Magic
const (
    magicReq byte = 0x80
    magicRes byte = 0x81
)

// Opcode
const (
    OpGet byte = iota
    OpSet
    OpAdd
    OpReplace
    OpDelete
    OpIncrement
    OpDecrement
    OpQuit
    OpFlush
    OpGetQ
    OpNoop
    OpVersion
    OpGetK
    OpGetKQ
    OpAppend
    OpPrepend
    OpStat
    OpSetQ
    OpAddQ
    OpReplaceQ
    OpDeleteQ
    OpIncrementQ
    OpDecrementQ
    OpQuitQ
    OpFlushQ
    OpAppendQ
    OpPrependQ
    OpRequest
)

// Status
const (
    StatusNoError uint16 = iota
    StatusNotFound
    StatusKeyExists
    StatusValueTooLarge
    InvalidArguments
    ItemNotStored
    IncrDecrOnNonMetricValue
)

type ReqHeader struct {
    Opcode byte
    KeyLength uint16
    ExtrasLength uint8
    DataType byte
    Reserved []byte
    TotalBodyLength uint32
    Opaque []byte
    CAS uint64
}

func newReqHeader(buf []byte) *ReqHeader {
    return &ReqHeader{
        Opcode: buf[1],
        KeyLength: binary.BigEndian.Uint16(buf[2:4]),
        ExtrasLength: uint8(buf[4]),
        DataType: buf[5],
        Reserved: buf[6:8],
        TotalBodyLength: binary.BigEndian.Uint32(buf[8:12]),
        Opaque: buf[12:16],
        CAS: binary.BigEndian.Uint64(buf[16:24]),
    }
}

func ReadReqHeader(conn *net.TCPConn) (*ReqHeader, error) {
    buf, err := util.TcpRead(conn, headerSize)
    if err != nil {
        return nil, err
    }
    if buf[0] != magicReq {
        return nil, errors.New("invalid magic")
    }

    return newReqHeader(buf), nil
}

func WriteRes(conn *net.TCPConn, opcode byte, status uint16, opaque []byte, cas uint64, flags []byte, key []byte, value []byte) error {
    flagsLen := computeFlagsLen(flags)
    keyLen := computeKeyLen(key)

    res := make([]byte, computeResSize(flags, key, value))
    res[0] = magicRes
    res[1] = opcode
    binary.BigEndian.PutUint16(res[2:4], keyLen)
    res[4] = flagsLen // extra length
    res[5] = 0 // data type
    binary.BigEndian.PutUint16(res[6:8], status)
    binary.BigEndian.PutUint32(res[8:12], computeTotalBodyLen(flags, key, value))
    copy(res[12:16], opaque)
    binary.BigEndian.PutUint64(res[16:headerSize], cas)

    if flags != nil {
        copy(res[headerSize:headerSize + flagsLen], flags)
    }
    if key != nil {
        copy(res[(headerSize + flagsLen):(headerSize + uint16(flagsLen) + keyLen)], key)
    }
    if value != nil {
        copy(res[(headerSize + uint16(flagsLen) + keyLen):], value)
    }

    _, err := conn.Write(res)
    return err
}

func computeResSize(flags []byte, key []byte, value []byte) uint32 {
    return headerSize + computeTotalBodyLen(flags, key, value)
}

func computeKeyLen(key []byte) uint16 {
    if key != nil {
        return uint16(len(key))
    }
    return 0
}

func computeFlagsLen(flags []byte) uint8 {
    if flags != nil {
        return uint8(4)
    }
    return 0
}

func computeTotalBodyLen(flags []byte, key []byte, value []byte) uint32 {
    ret := 0
    if flags != nil {
        ret += len(flags)
    }
    if key != nil {
        ret += len(key)
    }
    if value != nil {
        ret += len(value)
    }
    return uint32(ret)
}
