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
		n, err := conn.Read(inputData)
		if err != nil {
			fmt.Println("Error reading bytes")
			return
		}
		if n == 0 {
			return
		}

		writeResponse, err := RESPParser(inputData)

		if err != nil {
			fmt.Println("Error to run function RESPParser")
		}
		conn.Write([]byte(writeResponse))

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

	fmt.Println("ParsedData ", parsedData)
	if err != nil {
		fmt.Println("Error parsing the input bytes in function RESPParser ")
	}

	for i := 0; i < len(parsedData); i++ {
		if parsedData[i] == "echo" {
			echoFlag = true
		} else if parsedData[i] == "ping" {
			return "$4\r\nPONG\r\n", nil
		}
		if echoFlag == true {
			response = response + string(parsedData[i])
		}

	}
	//fmt.Println("Resp Parser response ", response)
	a := len(parsedData) // length of the array
	fmt.Println("Length of parsedData is len(parsedData[a-1]) ", len(parsedData[a-1]))
	result := reponsePrefix + strconv.Itoa(len(parsedData[a-1])) + "\r\n" + parsedData[a-1] + responsePostfix

	fmt.Println("The result of the operation is ", result)
	return result, nil

}

func ParseString(input1 []byte) (string, error) {

	//fmt.Println("function string values ", string(input))
	// function to parse strings
	string1 := ""
	i := 0
	for i < 20 {
		//fmt.Println("Inside the FUNC parseString ", i)
		if input1[0+i] == '$' {
			//this is a bulk string
			slen := string(rune(input1[i+1]))
			len, _ := strconv.ParseInt(slen, 10, 64)
			fmt.Println("Bulk string is TRUE", len)
			// \r -3
			// \n - 4
			string1 = string(input1[4+i : 4+int(len)+i])
			fmt.Println("Parse String String1", len, string1)
			break
		}
		i++

	}

	return string1, nil

}

func ParseArray(input []byte) ([]string, error) {
	//function to parse array elements

	element := []string{}

	if input[0] == '*' {
		//this is an array
		//fmt.Println("Inside function parsed Array len", string(rune(input[1])))
		len1 := string(rune(input[1]))
		len2, _ := strconv.ParseInt(len1, 10, 64) //number of items in the array

		//pos 2 -- \r
		//pos 3 -- \n
		//j := 4

		for i := 0; i < int(len2); i++ {
			element1, err := ParseString(input[4+i*2:])
			if err != nil {
				fmt.Println("Failed to parse string ")
			}
			element = append(element, element1)

		}
		//fmt.Println("inside func array array is ", element)

		//}

	} else {
		return []string{}, errors.New("Inside ParseArray the passed byte does not follow redis encoding")
	}
	return element, nil
}
