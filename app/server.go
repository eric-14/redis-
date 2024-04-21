package main

import (
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
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

		conn, err := listener.Accept()

		if err != nil {
			fmt.Println("Failed to accept new clients in the TCP server ")
			return
		}
		defer conn.Close()

		go handleConn1(conn)

	}

}

func handleConn1(conn net.Conn) {

	//defer conn.Close()

	//fmt.Println("Handle connection function")

	for {

		inputData := make([]byte, 1024) // buffer to read multiple inputs

		n, error := conn.Read(inputData)

		if error != nil {
			fmt.Println("Error reading bytes")
			return
		}

		// iterate through the message to get info PING
		//message := "+PONG\r\n"
		//responseMessage := []byte(message)
		// numberOfPings := 1

		if n == 0 {
			return
		}
		writeResponse, err := RESPParser(inputData)

		if err != nil {
			fmt.Println("Error to run function RESPParser")
		}
		conn.Write([]byte(writeResponse))
		// fmt.Println(string(inputData))

		// for i := 0; i < n; i++ {

		// 	if inputData[i] == 'p' && inputData[i+1] == 'i' && inputData[i+2] == 'n' && inputData[i+3] == 'g' { //PING MESSAGE

		// 		conn.Write([]byte("+PONG\r\n"))
		// 		fmt.Println("Responding with pong ", i, n)

		// 	}
		// }
		//conn.Write([]byte("+PONG\r\n"))

	}

}

func RESPParser(input []byte) (string, error) {
	/*
		@supporting arrays and string
		***Arrays format is *<number-of-elements>\r\n<element-1>...<element-n>
		*** Bulk Strings formart is $<length>\r\n<data>\r\n


	*/

	reponsePrefix := "$"      // number of characters in the prefix are 6
	responsePostfix := "\r\n" // number of characters in the postfix are 4
	response := ""
	echoFlag := false

	parsedData, err := ParseArray(input)

	if err != nil {
		fmt.Println("Error parsing the input bytes in function RESPParser ")
	}

	for i := 0; i < len(parsedData); i++ {
		if parsedData[i] == "echo" {
			echoFlag = true
		}
		if echoFlag == true {
			response = response + string(parsedData[i])
		}

	}
	fmt.Println("Resp Parser response ", response)

	result := reponsePrefix + "4" + response + responsePostfix

	return result, nil

}

func ParseString(input []byte) (string, error) {

	//fmt.Println("function string values ", string(input))
	// function to parse strings
	string1 := ""
	if input[0] == '$' {
		//this is a bulk string
		//
		slen := string(rune(input[1]))
		len, _ := strconv.ParseInt(slen, 10, 64)

		// \r -3
		// \n - 4

		// for i := 0; i < int(len); i++ {

		// 	//appending characters to form the first
		// 	if input[i] != '\r' && input[i] != '\n' {
		// 		string1 = string1 + string(input[i])
		// 	}

		// }

		string1 = string(input[5 : 5+len])
		fmt.Println("Inside parse string function ", string1)

	} else {
		return "", errors.New("From Parse String input not bulk string ")
	}
	return string1, nil
}

func ParseArray(input []byte) ([]string, error) {
	//function to parse array elements
	fmt.Println("Parsing array elements")

	element := []string{}

	if input[0] == '*' {
		//this is an array
		fmt.Println("Inside function parsed Array len", string(rune(input[1])))
		len1 := string(rune(input[1]))
		len2, _ := strconv.ParseInt(len1, 10, 64) //number of items in the array

		// pos 2 -- \r
		//pos 3 -- \n
		j := 4
		//iterate upto \r\n to find the first element
		//append the element to the arrays
		if input[j] == '$' {
			//next element is a bulk string
			for i := 0; i < int(len2); i++ {
				// iterate over the array elements
				//parse String returns the element i
				element1, err := ParseString(input[4:])
				if err != nil {
					fmt.Println("Failed to parse string ")
				}
				element = append(element, element1)

			}

		}

	} else {
		return []string{}, errors.New("Inside ParseArray the passed byte does not follow redis encoding")
	}
	return element, nil
}

// func echoParser(input []byte) {
// 	//accepts input as byte and should reply with the same message

// 	echoFlag := false

// 	reponsePrefix := "$3\r\n" // number of characters in the prefix are 6
// 	responsePostfix := "\r\n" // number of characters in the postfix are 4
// 	response := ""

// 	for i := 0; i < len(input); i++ {

// 		/*
// 			In ascii tables capital letters are from 65 - 90
// 			and small caps are fromm 97 - 122

// 			so in this code we will use small letters
// 			if a character is capital we convert it to small by adding 32
// 		*/

// 		if int(input[i]) >= 65 && int(input[i]) <= 90 {
// 			value := int(input[i]) + 32
// 			input[i] = byte(value)
// 		} // all strings are now converted to small caps

// 	}

// 	for i := 0; i < len(input); i++ {

// 		//now find the echo message
// 		if input[i] == 'e' && input[i+1] == 'c' && input[i+2] == 'h' && input[i+3] == 'o' {
// 			//received an echo command shouldd reply with the same message
// 			echoFlag = true

// 		}

// 		if echoFlag == true {
// 			// finding string.
// 			for j := i + 1; j < len(input); j++ {
// 				if input[j] >= 48 && input[j] <= 90 {
// 					//message must be an alphabet or number, : ,; < , = > , ? @, ^
// 					response = string(input[j])
// 				}

// 				if input[j] == '\r' || input[j] == '\n' {
// 					break
// 					//this is not the string
// 				}
// 			}

// 		}

// 	}
// }
