package main

import (
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

var dictionary map[string]string

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

	dictionary = make(map[string]string)

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
		fmt.Println("Error parsing the function clea ")
	}

	for i := 0; i < len(parsedData); i++ {
		if parsedData[i] == "echo" {
			echoFlag = true
		} else if parsedData[i] == "set" {
			executingFunction(0, parsedData[i+1], parsedData[i+2])
			return "+OK\r\n", nil
		} else if parsedData[i] == "get" {
			res12, err := executingFunction(1, parsedData[i+1], "")
			if err != nil {
				fmt.Println("Error executing get function")
			}
			return "$" + strconv.Itoa(len(res12)) + "\r\n" + res12 + "\r\n", nil
		} else if parsedData[i] == "ping" {
			return "$4\r\nPONG\r\n", nil
		} else if parsedData[i] == " " {
			return "+OK\r\n", nil
		} else if parsedData[i] != " " {
			// parsed array has returned a value then
			len1 := strconv.Itoa(len(parsedData[i]))
			fmt.Println("line 101   ")
			return "$" + len1 + "\r\n" + parsedData[i] + "\r\n", nil
		}

		if echoFlag == true {
			response = response + string(parsedData[i+1])
		}

	}
	//fmt.Println("Resp Parser response ", response)
	a := len(parsedData) // length of the array
	fmt.Println("Length of parsedData is len(parsedData[a-1]) ", len(parsedData[a-1]))
	result := reponsePrefix + strconv.Itoa(len(parsedData[a-1])) + "\r\n" + parsedData[a-1] + responsePostfix

	fmt.Println("The result of the operation is ", result)
	return result, nil

}

func ParseString(input1 []byte, count1 int) (string, error) {

	//fmt.Println("function string values ", string(input))
	// function to parse strings
	string1 := ""
	count2 := 0
	i := 0
	for {
		//fmt.Println("Inside the FUNC parseString ", i)
		if input1[i] == '$' {

			// increment counter

			if count1 == count2 {
				// this is the unparsed string
				//this is a bulk string
				slen := string(rune(input1[i+1]))
				len, _ := strconv.ParseInt(slen, 10, 64)
				//fmt.Println("Bulk string is TRUE", len, count1, count2)
				// \r -3
				// \n - 4
				string1 = string(input1[4+i : 4+int(len)+i])
				//fmt.Println("Parse String String1", len, string1)
				break
			} //else if count2 > counter
			count2++
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
		arrayLen, _ := strconv.ParseInt(len1, 10, 64) //number of items in the array

		//pos 2 -- \r
		//pos 3 -- \n
		//j := 4

		for i := 0; i < int(arrayLen); i++ {
			element1, err := ParseString(input[4:], i)

			if err != nil {
				fmt.Println("Failed to parse string ")
			}
			element = append(element, element1)

		}
		//fmt.Println("inside func array array is ", element)

		//
	}
	//else {
	// 	res1, err := executingFunction(input)

	// 	if err != nil {
	// 		fmt.Println("Failed to execute function ")
	// 	}
	// 	return []string{res1}, nil
	// 	//return []string{}, errors.New("Inside ParseArray the passed byte does not follow redis encoding")
	// }
	return element, nil
}

func executingFunction(fn int, key string, value string) (string, error) {
	//implement set function

	if fn == 0 {
		// set function implementation

		// adding the key value pairs to db
		dictionary[key] = value
		return "", nil
	} else if fn == 1 {
		// implementing get function
		res1 := dictionary[key] // value in the dictionary
		return res1, nil
	}

	return "", nil
}

func keyValue(input []byte) ([]string, error) {
	string1 := ""
	string2 := ""
	result := []string{}
	counter := 0
	for i := 0; i < len(input); i++ {
		/*
			first value is key
			second value is value
		*/
		if input[i] == ' ' {
			counter++
		}

		if counter < 1 && input[i] != ' ' {
			string1 = string1 + string(input[i])

		} else if counter >= 1 && input[i] != ' ' {
			string2 = string2 + string(input[i])
		}

	}
	result[0] = string1
	result[1] = string2
	return result, nil
}
