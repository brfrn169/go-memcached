package util

import (
    "io"
    "net"
    "time"
)

func TcpRead(conn *net.TCPConn, len uint32) ([]byte, error) {
    buf := make([]byte, len)
    _, err := io.ReadFull(conn, buf)
    return buf, err
}

func GetNowMillis() uint64 {
    now := time.Now()
    return uint64(now.UnixNano() / 1000000)
}
