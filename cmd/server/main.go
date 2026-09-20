package main

import "im/internal/server"

func main() {
	serever := server.NewServer("127.0.0.1", "8888")

	serever.Start()
}
