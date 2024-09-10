package main

import (
	"fmt"
)

func main() {
	addr := "127.0.0.1:6969"

	fmt.Println("[START] > Server is up and running on: ", addr)
	// srv := httpServer.NewHttpServer(addr)
	// log.Fatal(srv.ListenAndServe())
}
