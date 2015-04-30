package main

import (
	"fmt"
	"os"
	"os/signal"
	"runtime"

	"github.com/brfrn169/go-memcached/server"
)

func main() {
	numCPU := runtime.NumCPU()
	runtime.GOMAXPROCS(numCPU)

	// TODO handle command line argument

	s := server.NewServer("localhost:11211")
	s.Start()

	fmt.Println("start")

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, os.Kill)
	<-c

	fmt.Println("stop")
}
