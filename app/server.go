package main

import (
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
	"sync"
	"time"
)

const (
	HOST1 = "localhost"

	TYPE1 = "tcp"
)

type ReplicaInfo struct {
	role               string
	connected_slaves   int
	master_replied     string
	ports              []string // ports running servers
	master_repl_offset int
	//Key is the master or replica + connected_slaves[which is the ID of the server]
	connObject map[string]*net.Listener // a map object used to track references to servers running

	second_repl_offset   int
	repl_backlog_active  int
	repl_backlog_size    int
	repl_backlog_histlne int
}

var PORT string

var dictionary map[string]string
var numberOfReplica int

// controls details of replicas
var replicainfo ReplicaInfo

type Data1 struct {
	value      string
	expiryTime string
	timeNow    time.Time
}

var timetracker1 map[string]Data1
var dataEmpty Data1
var data Data1

func main() {
	var wg sync.WaitGroup

	PORT = "6379"
	numberOfReplica = 0 // master position or ID is 0

	replicainfo.role = "master"
	replicainfo.connected_slaves = numberOfReplica
	replicainfo.master_replied = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
	replicainfo.master_repl_offset = 0
	replicainfo.ports = []string{"flag"}
	replicainfo.connObject = make(map[string]*net.Listener)

	if len(os.Args) >= 2 {
		newPort, err := portReplica(os.Args)
		if err != nil {
			fmt.Println("error in replicating ports")
			return
		}

		newReplica("tcp", HOST1, newPort)

		fmt.Println("Line 63 number of server ports after creating new replica  ", replicainfo.ports)
	}

	serverExits := false
	for _, port := range replicainfo.ports {
		//fmt.Println("line 80 server ports being tracked ", port, PORT)
		if port == PORT {
			fmt.Println("line 82 port already exists in server", port, PORT)
			serverExits = true
		}
	}
	if !serverExits {
		// starting server master
		listener, err := net.Listen(TYPE1, HOST1+":"+PORT)
		if err != nil {
			fmt.Println("line 90 Failed to open a TCP port ", err)
			return
			// log.Fatal(err)
			// os.Exit(1)
		}
		//adding master to server tracker
		replicainfo.connObject[replicainfo.role+strconv.Itoa(replicainfo.connected_slaves)] = &listener
		fmt.Println("line 72 ConnObject after logging the master ", replicainfo.connObject)
		replicainfo.ports = append(replicainfo.ports, PORT)
		fmt.Println("line 98 logging new server port ", PORT, replicainfo.ports)

		fmt.Println("Line 98")
		defer listener.Close()
		wg.Add(1)

		go fnListen(&wg, &listener)
	} else if serverExits {
		// assume its a master 6379

	} // for {
	// 	conn, err := listener.Accept()

	// 	if err != nil {
	// 		fmt.Println("Failed to accept new clients in the TCP server ")
	// 		return
	// 	}
	// 	defer conn.Close()

	// 	go handleConn1(conn)

	// }

	//listen to incoming connectings and

	//fmt.Println("OS args are ", os.Args, len(os.Args))
	// openServer := false
	// fmt.Println("line 71 ports available ", replicainfo.ports, PORT)
	// for _, port := range replicainfo.ports {
	// 	if port == PORT {
	// 		fmt.Println("found matching ports")
	// 		openServer = true
	// 	}
	// }

	//if !openServer {

}
func fnListenReplica(wg *sync.WaitGroup, listener *net.Conn) {
	fmt.Println("line 136 func listen new replica. Listening for incoming connections to server")
	//fmt.Println("line 137 fnListenReplica The function should respond to master with REPLCONF flag")
	responseMaster := make([]byte, 300)
	message1 := "*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$4\r\n6380\r\n"
	message2 := "*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n"

	defer wg.Done()
	for {
		m, err := (*listener).Read(responseMaster)

		if err != nil {
			fmt.Println("line 147 Failed to listen to read master  ")
			return
		}
		listener1, err := net.Listen("tcp", "localhost:"+"6379")

		conn, err := listener1.Accept()

		conn.Read(responseMaster)

		fmt.Println("line 156 ", responseMaster)
		// defer conn.Close()
		if err != nil {
			fmt.Println("line 154 failed to listen to server")
		}
		masterResponded := false

		if m > 0 {
			// we received a response from the master
			resp, err3 := ParseArray(responseMaster)
			if err3 != nil {
				fmt.Println("line 195 failed to parse bytes from master")
			}
			for _, elem := range resp {
				// parse the elements in the array
				if elem == "PONG" {
					masterResponded = true
					fmt.Println("line 200 master responded to  PING")
				}
			}
			if masterResponded == true {
				// master responded
				// reply with message that
				(*listener).Write([]byte(message1))
				time.Sleep(100)
				(*listener).Write([]byte(message2))
			}
		}

		if err != nil {
			fmt.Println("line 197 error in reading bytes from new Replica")
		}

	}

}
func fnListen(wg *sync.WaitGroup, listener *net.Listener) {
	fmt.Println("line 125 func listen. Listening for incoming connections to listener ")
	defer wg.Done()
	for {
		conn, err := (*listener).Accept()

		if err != nil {
			fmt.Println("Failed to accept new clients in the TCP server ")
			return
		}
		defer conn.Close()

		handleConn1(conn)

	}

}

func newReplica(TYPE string, HOST string, PORT string) int {
	fmt.Println("line 155 Fn newReplica  ", TYPE, HOST, PORT)

	for _, port := range replicainfo.ports {
		if port == PORT {
			//if port already exists the server should not try to recreate it
			fmt.Println("Line 100 cant open port already opened returning to caller ", port, PORT)
			return 1
		}
	}
	listener, err := net.Dial(TYPE, HOST+":"+"6379")
	replicainfo.ports = append(replicainfo.ports, "6379")
	numberOfReplica++
	// adding a slave to the object tracker

	//replicainfo.connObject[replicainfo.role+strconv.Itoa(replicainfo.connected_slaves+1)] = &listener

	//adding replica information

	if err != nil {
		fmt.Println("line 165 Failed to open a TCP port ", err)
		//log.Fatal(err)
		//os.Exit(1)
		return -1
	}
	// if server is successful log its information

	replicainfo.ports = append(replicainfo.ports, PORT)

	fmt.Println("Line 240 fn newReplica logged port for new server ", replicainfo.ports)

	//for {
	message := "*1\r\n$4\r\nping\r\n"

	//conn, err := listener.Accept()
	listener.Write([]byte(message))

	var wg1 sync.WaitGroup
	wg1.Add(1)
	go fnListenReplica(&wg1, &listener)

	// if err != nil {
	// 	fmt.Println("Failed to accept new clients in the TCP server ")
	// 	return numberOfReplica
	// }
	// defer listener.Close()
	fmt.Println("Line 218 Fn returning newReplica")

	//go handleConn1(conn)
	//}
	err2 := listener.Close()
	if err2 != nil {
		fmt.Println("line 173 error in closing listener in FN newReplica", err2)
	}
	return 1
}

func portReplica(input []string) (string, error) {
	//func identifies a port number and can
	//replicate the code to that port
	fmt.Println("received input", input, len(input))
	foundPort := false
	string1 := ""
	fmt.Println("line 141 ", input)
	for i := 0; i < len(input); i++ {

		if input[i] == "--port" {
			foundPort = true
			string1 = input[i+1]
		} else if input[i] == "--replicaof" {
			fmt.Println("line 146 --replicaof setting replicainfo to slave ")
			replicainfo.role = "slave"
			replicaExists := false
			for _, port := range replicainfo.ports {
				if port == string1 {
					fmt.Println("line 286 replica exists", port, string1)
					replicaExists = true
					return "", nil
				}
			}
			if replicaExists {
				replicaPing("localhost", input[i+2])
			} else {
				fmt.Println("Line 262 adding a new replica first since one does not exist ")
				newReplica("tcp", "localhost", string1)
			}

			//return string1, nil

			//fmt.Println("line 129 ", replicainfo.role)
		} else if foundPort == false {
			//return "", nil
			continue
		}
	}
	return string1, nil
}

func handleConn1(conn net.Conn) {

	//defer conn.Close()
	//fmt.Println("Handle connection function")

	dictionary = make(map[string]string)
	timetracker1 = make(map[string]Data1)

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
	// echoFlag := false

	parsedData, err := ParseArray(input)

	fmt.Println("ParsedData 100", parsedData)
	if err != nil {
		fmt.Println("Error parsing the function clea ")
	}

	for i := 0; i < len(parsedData); i++ {
		if parsedData[i] == "ECHO" {
			response = response + string(parsedData[i+1])
		} else if parsedData[i] == "set" || parsedData[i] == "SET" {
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
		} else if parsedData[i] == "GET" {
			res12, err := executingFunction(1, parsedData[i+1], "")
			fmt.Println("executing get")

			if err != nil {
				fmt.Println("Error executing get function")
				return "$-1\r\n", nil
			}
			return "$" + strconv.Itoa(len(res12)) + "\r\n" + res12 + "\r\n", nil
		} else if parsedData[i] == "ping" || parsedData[i] == "PING" {
			return "$4\r\nPONG\r\n", nil
		} else if parsedData[i] == "INFO" {
			//returning replication information about the server
			//fmt.Println("line 213 found INFO")
			dest, nil := ReplicaInformation()

			return dest, nil

		}
		//  else if parsedData[i] == "--replicaof" {
		// 	//&& replicainfo.role == "slave"
		// 	fmt.Println("line 245 PING Master")
		// 	replicaPing(parsedData[i+1], parsedData[i+2])
		// }

	}

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
		if input1[i] == '$' {
			if count1 == count2 {
				j := i + 1
				slen := 0
				len := ""
				for {
					if input1[j] == '\r' {
						i = j + 1
						break
					}

					len = len + string(input1[j])
					j++
					//slen = slen + string(input1[j+1])
				}
				slen, _ = strconv.Atoi(len)

				string1 = string(input1[i+1 : i+slen+1])
				break
			}

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
		arrayLen, _ := strconv.ParseInt(len1, 10, 64)
		//number of items in the array

		//pos 2 -- \r
		//pos 3 -- \n
		//j := 4

		for i := 0; i < int(arrayLen); i++ {
			element1, err := ParseString(input[3:], i)
			//	fmt.Println("line 314 ", input[4:])

			if err != nil {
				fmt.Println("Failed to parse string ")
			}
			element = append(element, element1)

		}
	}
	return element, nil
}

func executingFunction(fn int, key string, value string) (string, error) {

	if fn == 0 {
		// set function implementation
		// adding the key value pairs to db
		dictionary[key] = value
		return "", nil
	} else if fn == 1 {
		// implementing get function

		res1 := dictionary[key] // value in the dictionary
		fmt.Println("line 231 ", res1)
		dataEmpty := Data1{}

		timerdata := timetracker1[key]
		fmt.Println("line 239 result from get fn", key, timerdata, dataEmpty)
		if timerdata != dataEmpty {
			fmt.Println("line 241 time time tracker in fn get ", timetracker1[key])
			// if the data type has a time tracker then execute time function
			timedata := timetracker1[key] // time information about
			expTime, _ := strconv.Atoi(timedata.expiryTime)

			duration := time.Since(timedata.timeNow)

			//time.Now().UTC() > time.sub(timedata.timeNow, (time.Duration(expTime) * time.Millisecond))

			if duration > time.Duration(expTime)*time.Millisecond {
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

func timetracker(fn int, key string, value1 string, nowTime time.Time, expiryTime1 string) (string, error) {

	string1, _ := executingFunction(0, key, value1)
	fmt.Println("line 277", string1)
	data = Data1{
		value:      value1,
		expiryTime: expiryTime1,
		timeNow:    nowTime,
	}

	//fmt.Println("line 275 ",fn , key, value1 ,expiryTime1, string1, nowTime, &data)
	timetracker1[key] = data
	fmt.Println("Line 302 fn timetracker", key, timetracker1[key])
	return "+OK\r\n", nil
}

//implement set function

func ReplicaInformation() (string, error) {
	fmt.Println("line 374 printing the role ", replicainfo.role)
	string1 := "role:" + replicainfo.role
	string2 := "master_replid:" + replicainfo.master_replied
	string4 := "master_repl_offset:" + strconv.Itoa(replicainfo.master_repl_offset)
	//$" + strconv.Itoa(len(string1)) + "\r\n" + string1 + "\r\n" + "$" + strconv.Itoa(len(string2)) + "\r\n" + string2 + "\r\n" +
	string3 := "$" + strconv.Itoa(len(string1+string2+string4)+2) + "\r\n" + string1 + "," + string2 + "," + string4 + "\r\n"
	//fmt.Println("line 379 ", string3)
	return string3, nil
}

func replicaPing(host string, port string) (string, error) {
	/*
		Function that replicas use to ping master
	*/
	fmt.Println("line 457 logging servers inside map  tracker ", replicainfo.connObject)
	Listener := replicainfo.connObject["master0"]
	//conn, err := net.Dial("tcp", host+":"+port)
	replicainfo.ports = append(replicainfo.ports, port)
	fmt.Println("\n line 444 added a new port in replicaPIng", replicainfo.ports)
	// if err != nil {
	// 	fmt.Println("Falied to open server")
	// }
	//defer conn.Close()
	message := "*1\r\n$4\r\nping\r\n"

	conn, err := (*Listener).Accept()
	if err != nil {
		fmt.Println("line 462 could not start connection to server ")
	}
	defer conn.Close()
	conn.Write([]byte(message))
	//CRLF := "\r\n"
	return port, nil

	//return "*3\r\n$4\r\nping\r\n" + "$" + strconv.Itoa(len(host)) + CRLF + host + CRLF + "$" + strconv.Itoa(len(port)) + CRLF + port + CRLF, nil
}
