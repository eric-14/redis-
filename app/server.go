package main

import (
	"fmt"
	"log"
	"net"
	"os"
)

const (
	HOST1 = "localhost"
	PORT1 = "6379"
	TYPE1 = "tcp"
)

func main() {
	listener, err := net.Listen(TYPE1, HOST1+":"+PORT1)

	if err != nil {

		fmt.Println("Failed to open a TCP port", err)
		log.Fatal(err)

		os.Exit(1)
	}
	defer listener.Close()
	for {

		conn, error := listener.Accept()

		if error != nil {
			fmt.Println("Failed to accept new clients in the TCP server ")
			return
		}
		defer conn.Close()

		handleConn1(conn)

	}

}

func handleConn1(conn net.Conn) {
	conn.Write([]byte("+PONG\r\n"))
	//defer conn.Close()

	//fmt.Println("Handle connection function")
	inputData := make([]byte, 1024) // buffer to read multiple inputs

	n, error := conn.Read(inputData)

	if error != nil {
		fmt.Println("Error reading bytes")
		return
	}

	// iterate through the message to get info PING
	message := "+PONG\r\n"
	responseMessage := []byte(message)
	numberOfPings := 1

	if n == 0 {
		return
	}
	i := 0

	// fmt.Println(string(inputData))
	//for i < n {

	if inputData[i] == 'p' && inputData[i+1] == 'i' && inputData[i+2] == 'n' && inputData[i+3] == 'g' { //PING MESSAGE

		conn.Write(responseMessage)
		fmt.Println("Responding with pong ", i, n, numberOfPings)
		i = 1 + 3

	}

}
