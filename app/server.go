package main


import (
	"fmt"
	"net"
	"log"
	"os"

)

const (
	HOST1 = "localhost"
	PORT1 = "8080"
	TYPE1 = "TCP"
)

/*
func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

	// Uncomment this block to pass the first stage
	//
	 l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}
	_, err = l.Accept()
	if err != nil {
 	fmt.Println("Error accepting connection: ", err.Error())
	os.Exit(1)
	 }
}
*/

func main(){
	listener, err := net.Listen(TYPE1, HOST1+":"+PORT1)

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

		handleConn1(conn)
		defer conn.Close()
}
}



func handleConn1(conn net.Conn){
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


