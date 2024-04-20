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
			log.Fatal("Failed to accept new clients in the TCP server \n")
			return
		}

		handleConn1(conn)
		defer conn.Close()
	}

}

func handleConn1(conn net.Conn) {
	//defer conn.Close()

	//fmt.Println("Handle connection function")
	inputData := make([]byte, 15) // buffer to read multiple inputs

	n, error := conn.Read(inputData)

	if error != nil {
		fmt.Println("Error reading bytes")
		return
	}

	// iterate through the message to get info PING
	message := "+PONG\r\n"
	responseMessage := []byte(message)

	for i := 0; i < len(inputData); i++ {
		fmt.Println("Parsing message ", inputData)
		if inputData[i] == 'p' && inputData[i+1] == 'i' && inputData[i+2] == 'n' && inputData[i+3] == 'g' { //PING MESSAGE
			conn.Write(responseMessage)
			fmt.Println("Responding with pong", i, n)

			/*
				after responding to the first ping
				continue will restart the loop and after reading the second ping
				the client will receive another PONG
			*/
			return
		}
	}

}
