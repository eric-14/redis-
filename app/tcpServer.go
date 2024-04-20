/*package main 

import (
	"fmt"
	"net"
	"log"
	"os"

)

const (
	HOST = "localhost"
	PORT = "8080"
	TYPE = "TCP"
)


func main(){
	listener, err := net.Listen(TYPE, HOST+":"+PORT)

	if err != nil {
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

		handleConn(conn)
		defer conn.Close()
}
}



func handleConn(conn net.Conn){
	defer conn.Close()

	inputData := make([]byte, 1024)

	n, error := conn.Read(inputData)

	if error != nil {
		fmt.Println("Error reading bytes")
		return 
	}

	// iterate through the message to get info PING
	message:= "PONG\r\n"
	responseMessage := []byte(message)
	
	for i:=0; i < n; i++ {
		if inputData[i] == 'P' && inputData[i+1] =='i' && inputData[i+2] =='n' && inputData[i+3] == 'g' { //PING MESSAGE
			conn.Write(responseMessage)
		}
	}

}

*/