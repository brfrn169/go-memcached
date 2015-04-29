package server

import (
	"fmt"
	"net"
	"os"
	"time"

	"github.com/brfrn169/go-memcached/item"
)

type Server struct {
	laddr       string
	itemService *item.ItemService
}

func NewServer(laddr string) *Server {
	return &Server{
		laddr:       laddr,
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

		err = conn.SetDeadline(time.Now().Add(5 * time.Second))
		if err != nil {
			fmt.Printf("[%v]: %v\n", conn.RemoteAddr(), err)
			return
		}

		// TODO limit number of connection
		rp := NewRequestProcessor(conn.(*net.TCPConn), s.itemService)
		rp.Start()
	}
}
