package main

import (
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"time"
	"errors"
)

const (
	HOST1 = "localhost"
	PORT1 = "6379"
	TYPE1 = "tcp"
)

var dictionary map[string]string

type Data1 struct {
	value string
	expiryTime string
	timeNow time.Time
}
var timetracker1 map[string]*Data1
var dataEmpty Data1
var data Data1


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
	timetracker1 = make(map[string]*Data1)

	reponsePrefix := "$"      // number of characters in the prefix are 6
	responsePostfix := "\r\n" // number of characters in the postfix are 4
	response := ""
	echoFlag := false

	parsedData, err := ParseArray(input)

	fmt.Println("ParsedData 100", parsedData)
	if err != nil {
		fmt.Println("Error parsing the function clea ")
	}

	for i := 0; i < len(parsedData); i++ {
		if parsedData[i] == "echo" {
			echoFlag = true
		} else if parsedData[i] == "set" {
			fmt.Println("line 116 adding timetracker fn", len(parsedData))
			if len(parsedData) > 3 {
				// there is more the keys and values in the entry 
				
				if parsedData[i+3] == "px" {
										
					string2, _ := timetracker(0, parsedData[i+1], parsedData[i+2], time.Now(), parsedData[i+4])
					fmt.Println("line 115", string2)
					return "+OK\r\n", nil
				}
			}

			executingFunction(0, parsedData[i+1], parsedData[i+2])

			
			return "+OK\r\n", nil
		} else if parsedData[i] == "get" {
			res12, err := executingFunction(1, parsedData[i+1], "")
			fmt.Println("executing get")
			// if err == errors.New("TIme passed") {
			// 	return "$-1\r\n", nil 
			// }
			if err != nil {
				fmt.Println("Error executing get function")
				return "$-1\r\n", nil 
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

	a := len(parsedData) 
	//fmt.Println("Length of parsedData is len(parsedData[a-1]) ", len(parsedData[a-1]))
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
			if count1 == count2 {
				slen := string(rune(input1[i+1]))
				len, _ := strconv.ParseInt(slen, 10, 64)
				//fmt.Println("Bulk string is TRUE", len, count1, count2)
				// \r -3
				// \n - 4
				string1 = string(input1[4+i : 4+int(len)+i])
				break
			} 
			
			count2++
		}
		i++
	}
	return string1, nil
}

func ParseArray(input []byte)([]string, error){
	//function to parse array elements
	element := []string{}
	if input[0] == '*' {
		//this is an array
		//fmt.Println("Inside function parsed Array len", string(rune(input[1])))
		len1 := string(rune(input[1]))
		arrayLen, _ := strconv.ParseInt(len1, 10, 64) 
		//number of items in the array

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
	}
	return element, nil
}

func executingFunction(fn int, key string, value string) (string, error){
	

	if fn == 0 {
		// set function implementation
		// adding the key value pairs to db
		dictionary[key] = value
		return "", nil
	} else if fn == 1 {
		// implementing get function
		
		res1 := dictionary[key] // value in the dictionary
		
		dataEmpty := Data1{}
		
		timerdata := timetracker1[key] 
		fmt.Println("line 239 result from get fn", timerdata, dataEmpty)
		if timerdata != nil {
			fmt.Println("line 241 time time tracker in fn get ", *timetracker1[key])
			// if the data type has a time tracker then execute time function 
			timedata := *timetracker1[key]  // time information about 
			expTime,_ :=  strconv.Atoi(timedata.expiryTime)



			duration := time.Since(timedata.timeNow)

			//time.Now().UTC() > time.sub(timedata.timeNow, (time.Duration(expTime) * time.Millisecond))
			
			if duration >  time.Duration(expTime)*time.Millisecond {
				//time has elapsed 
				return "-1", errors.New("Expired Value passed")
			}
			fmt.Println("line 241 ", res1)
			
		}
		return res1, nil
		
	return "", nil
	}
	return "", nil
}

func keyValue(input []byte)([]string, error) {
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

func timetracker(fn int, key string, value1 string,nowTime time.Time,expiryTime1 string )(string, error){ 
	
	string1, _ := executingFunction(0, key, value1)
	fmt.Println("line 277", string1)
	data = Data1{
		value: value1,
		expiryTime: expiryTime1,
		timeNow: nowTime, 
		 
		 
	}
	fmt.Println("Line 291 fn timetracker", data)
	//fmt.Println("line 275 ",fn , key, value1 ,expiryTime1, string1, nowTime, &data)
	timetracker1[key]=&data
	return "+OK\r\n", nil
}


//implement set function
	