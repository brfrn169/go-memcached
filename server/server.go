package server

import (
    "net"
    "fmt"
    "time"
    "os"
    "encoding/binary"

    "github.com/brfrn169/go-memcached/item"
    "github.com/brfrn169/go-memcached/util"
)

type Server struct {
    laddr string
    itemService *item.ItemService

}

func NewServer(laddr string) *Server {
    return &Server{
        laddr: laddr,
        itemService: item.NewItemService(),
    }
}

func (s *Server) Start() {
    fmt.Println("start")
    go s.Service()
}

func (s *Server) Service() {
    listener, err := net.Listen("tcp", "localhost:11211")
    if err != nil {
        fmt.Printf("Listen Error: %v\n", err)
        os.Exit(1)
    }

    for {
        conn, err := listener.Accept()
        if err != nil {
            fmt.Printf("Accept Error: %v\n", err)
            continue
        }
        fmt.Printf("Accept [%v]\n", conn.RemoteAddr())

        // TODO pooling to limit connection
        go s.doProcess(conn.(*net.TCPConn))
    }
}

func (s *Server) doProcess(conn *net.TCPConn) {
    var err error

    defer conn.Close()

    err = conn.SetDeadline(time.Now().Add(5*time.Second))
    if err != nil {
        fmt.Printf("[%v]: %v\n", conn.RemoteAddr(), err)
        return
    }

    for {
        reqHeader, err := ReadReqHeader(conn)
        if err != nil {
            fmt.Printf("Read Header Error [%v]: %v\n", conn.RemoteAddr(), err)
            return
        }

        if reqHeader.Opcode == OpGet {
            err = s.get(conn, reqHeader)

            if err != nil {
                fmt.Printf("get Error [%v]: %v\n", conn.RemoteAddr(), err)
                return
            }
        } else if reqHeader.Opcode == OpSet {
            err = s.set(conn, reqHeader)

            if err != nil {
                fmt.Printf("set Error [%v]: %v\n", conn.RemoteAddr(), err)
                return
            }
        }
    }
}

func (s *Server) get(conn *net.TCPConn, reqHeader *ReqHeader) error {
    key, err := util.TcpRead(conn, uint32(reqHeader.KeyLength))
    if err != nil {
        return err
    }

    i := s.itemService.Get(key)

    fmt.Printf("key:%v, value:%v, flags:%v, cas:%v\n", i.Key, i.Value, i.Flags, i.Cas)

    // TODO status

    err = WriteRes(conn, OpGet, StatusNoError, reqHeader.Opaque, i.Cas, i.Flags, nil, i.Value)
    if err != nil {
        return err
    }
    return nil
}

func (s *Server) set(conn *net.TCPConn, reqHeader *ReqHeader) error {
    flags, err := util.TcpRead(conn, uint32(4))
    if err != nil {
        return err
    }

    expiry, err := util.TcpRead(conn, uint32(4))
    if err != nil {
        return err
    }

    key, err := util.TcpRead(conn, uint32(reqHeader.KeyLength))
    if err != nil {
        return err
    }

    valueLen := reqHeader.TotalBodyLength - uint32(reqHeader.ExtrasLength) - uint32(reqHeader.KeyLength)
    value, err := util.TcpRead(conn, valueLen)
    if err != nil {
        return err
    }

    // TODO cas, status

    fmt.Printf("key:%v, value:%v, flags:%v, expiry:%v\n", key, value, flags, expiry)

    i := s.itemService.Set(key, value, flags, binary.BigEndian.Uint32(expiry))
    err = WriteRes(conn, OpSet, StatusNoError, reqHeader.Opaque, i.Cas, nil, nil, nil)
    if err != nil {
        return err
    }

    return nil
}
