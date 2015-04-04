package main

import (
    "runtime"
    "os"
    "os/signal"

    "github.com/brfrn169/go-memcached/server"
)

func main() {
    numCPU := runtime.NumCPU()
    runtime.GOMAXPROCS(numCPU)

    // TODO handle command line argument

    s := server.NewServer("localhost:11211")
    s.Start()

    c := make(chan os.Signal, 1)
    signal.Notify(c, os.Interrupt, os.Kill)
    <-c
}
