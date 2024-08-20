package main

import (
	"fmt"
	"log"

	"github.com/n0tB0b17/distri/internal/server"
)

func main() {
	addr := "127.0.0.1:6969"

	fmt.Println("[START] > Server is up and running on: ", addr)
	srv := server.NewHttpServer(addr)
	log.Fatal(srv.ListenAndServe())
}
