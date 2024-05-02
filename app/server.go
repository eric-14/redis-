package main

import (
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
<<<<<<< HEAD
	"sync"
	"time"
=======
>>>>>>> 4b8418c4af96120b5f3595e6c0cdf5d3ae2b99ea
)

const (
	HOST1 = "localhost"

	TYPE1 = "tcp"
)

<<<<<<< HEAD
type ReplicaInfo struct {
	role               string
	connected_slaves   int
	master_replied     string
	ports              []string // ports running servers
	master_repl_offset int
	//Key is the master or replica + connected_slaves[which is the ID of the server]
	connObject map[string]*net.Conn // a map object used to track references to servers running

	// second_repl_offset   int
	// repl_backlog_active  int
	// repl_backlog_size    int
	// repl_backlog_histlne int
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

// var dataEmpty Data1
var data Data1
var wg1 sync.WaitGroup
var wg4 sync.WaitGroup

var mx sync.Mutex
=======
var dictionary map[string]string
>>>>>>> 4b8418c4af96120b5f3595e6c0cdf5d3ae2b99ea

func main() {
	fmt.Println("inside main fn ")

<<<<<<< HEAD
	replicainfo.role = "master"
	replicainfo.connected_slaves = numberOfReplica
	replicainfo.master_replied = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
	replicainfo.master_repl_offset = 0
	replicainfo.ports = make([]string, 20)
	replicainfo.connObject = make(map[string]*net.Conn)

	serverExits := false
	masterPORT := "6379"
	numberOfReplica = 0 // master position or ID is 0

	if len(os.Args) >= 2 {
		go portReplica(os.Args, &mx)
		// if err != nil {
		// 	fmt.Println("error in replicating ports ")
		// 	return
		// }

		//newReplica("tcp", HOST1, newPort)

=======
	if err != nil {
		fmt.Println("Failed to open a TCP port", err)
		log.Fatal(err)
		os.Exit(1)
>>>>>>> 4b8418c4af96120b5f3595e6c0cdf5d3ae2b99ea
	}
	fmt.Println("Line 63 number of server ports after creating new replica  ", replicainfo.ports)

<<<<<<< HEAD
	for _, port := range replicainfo.ports {
		//fmt.Println("line 80 server ports being tracked ", port, "masterPORT ", masterPORT)
		if port == masterPORT {
			fmt.Println("line 81 port already exists in server", port, masterPORT)
			serverExits = true
			//return
		}

	}

	if serverExits == false {
		// 	// starting server master
		// 	if replicainfo.connObject["master0"] != nil {
		listen7, err7 := net.Listen(TYPE1, HOST1+":"+"6381")

		if err7 != nil {
			fmt.Println("line 90 Failed to open a TCP port ", err7)
			return
		}
		// 		defer listen7.Close()

		// 	//adding master to server tracker
		// 	//replicainfo.connObject[replicainfo.role+strconv.Itoa(replicainfo.connected_slaves)] = &listener
		fmt.Println("line 72 ConnObject after logging the master ", replicainfo.connObject)
		replicainfo.ports = append(replicainfo.ports, masterPORT)
		fmt.Println("line 98 logging new server port ", masterPORT, replicainfo.ports)

		//fmt.Println("Line 98")

		wg1.Add(1)

		go fnListen(&wg1, &listen7)

		wg1.Wait()

		//return
=======
		conn, err := listener.Accept()

		if err != nil {
			fmt.Println("Failed to accept new clients in the TCP server ")
			return
		}
		defer conn.Close()

		go handleConn1(conn)

>>>>>>> 4b8418c4af96120b5f3595e6c0cdf5d3ae2b99ea
	}
	if serverExits {
		// assume its a master 6379
		byte2 := make([]byte, 1024)
		byte3 := make([]byte, 1024)

		conn1 := replicainfo.connObject["master0"]
		conn2 := replicainfo.connObject["slave0"]

		(*conn1).Read(byte2)

		for {
			n1, err23 := (*conn2).Read(byte3)
			if err23 != nil {
				fmt.Println("line 128 failed to read data from slave object ", err23)
				break
			}
			if n1 == 0 {
				fmt.Println("line 133 slave object returned 0 bytes")
				continue
			}
			if n1 > 0 {
				fmt.Println("line 137 the following bytes read from the slave object ", string(byte3))
			}
		}
		(*conn2).Write([]byte("*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$4\r\n6380\r\n"))

		fmt.Println("line 129 slave connection activated in main function ", string(byte3))

		fmt.Println("line 119 master serverExists true ", string(byte2))

		//(*conn1).Write([]byte("*1\r\n$4\r\nping\r\n"))

		fmt.Println("Line 126 address of connection in handle conn ", (*conn1).LocalAddr().String())

		//handleConn1((*conn1))
		//(*conn1).Write([]byte("*1\r\n$4\r\nping\r\n"))

		(*conn1).Write([]byte("*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n"))
		(*conn1).Close()
		os.Exit(1)

	}
	//wg1.Wait()

}

<<<<<<< HEAD
func fnListenReplica(wg *sync.WaitGroup, listener *net.Conn) {
	fmt.Println("line 136 func listen new replica. Listening for incoming connections to server")
	//fmt.Println("line 137 fnListenReplica The function should respond to master with REPLCONF flag")
	responseMaster := make([]byte, 1024)

	for {
		m, err := (*listener).Read(responseMaster)
		defer (*listener).Close()
		if err != nil {
			fmt.Println("line 147 Failed to listen to read master Sleeping for 10 ", err)
			return
			time.Sleep(10)
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
				// (*listener).Write([]byte(message1))
				// time.Sleep(100)
				// (*listener).Write([]byte(message2))
			}
		}
		wg.Done()
		os.Exit(1)

	}

}
func fnListen(wg *sync.WaitGroup, listener *net.Listener) {

	fmt.Println("line 211 func listen. Listening for incoming connections to listener ")

	//for {
	conn, err := (*listener).Accept()

	if err != nil {
		fmt.Println("Failed to accept new clients in the TCP server ")
		return
	}
	//defer conn.Close()
	//go

	wg4.Add(1)

	go handleConn1(conn, &wg4)

	wg4.Wait()
	wg.Done()

	//}

}

func newReplica(wg *sync.WaitGroup, TYPE string, HOST string, PORT string, mx *sync.Mutex) int {
	defer wg.Done()
	message1 := "*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$4\r\n6380\r\n"
	message2 := "*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n"
	fmt.Println("line 213 Fn newReplica  ", TYPE, HOST, PORT)

	fmt.Println("line 198 new replica function ", replicainfo.ports)

	for _, port := range replicainfo.ports {
		if port == PORT {
			//if port already exists the server should not try to recreate it
			fmt.Println("Line 218 cant open port already opened returning to caller ", port, PORT)
			return 1
		}
	}
	listener1, err5 := net.Dial(TYPE, HOST+":"+"6379")
	if err5 != nil {
		fmt.Println("line 188 ", err5)
		return -1
	}
	//defer listener1.Close()

	mx.Lock()

	replicainfo.connObject["master0"] = &listener1
	fmt.Println("line 227 logged master to server ")

	replicainfo.ports = append(replicainfo.ports, "6379")
	replicainfo.ports = append(replicainfo.ports, PORT)

	mx.Unlock()

	numberOfReplica++
	// adding a slave to the object tracker

	//replicainfo.connObject[replicainfo.role+strconv.Itoa(replicainfo.connected_slaves+1)] = &listener

	//adding replica information

	// if server is successful log its information

	fmt.Println("Line 240 fn newReplica logged port for new server ", replicainfo.ports)

	//for {
	message := "*1\r\n$4\r\nping\r\n"
	for {
		mx.Lock()
		replicainfo.connObject["slave0"] = &listener1
		mx.Unlock()

		listener1.Write([]byte(message))

		read12 := make([]byte, 1024)

		m, err6 := listener1.Read(read12)

		if err6 != nil {
			fmt.Println("line 218 ", err6)
		}
		if m > 0 {
			// fmt.Println("line 221 ", read12)
			string23 := string(read12[1:5])
			// if err7 != nil {
			// 	fmt.Println("line 220 ", err7)
			// }
			listener1.Write([]byte(message1))

			fmt.Println("line 223 ", string23)
			input45 := make([]byte, 100)
			m1, er1 := listener1.Read(input45)
			if er1 != nil {
				fmt.Println("line 234 response from server ", er1)
			}
			if m1 > 0 {

				string30 := string(input45[1 : m1-2])
				fmt.Println("line 245 response 2 from master ", m1, string30)
				if string30 == "OK" {
					listener1.Write([]byte(message2))
				}

			}
		}

		return 1
	}

	// var wg1 sync.WaitGroup
	// wg1.Add(1)
	//go fnListenReplica(&wg1, &listener1)

	// if err != nil {
	// 	fmt.Println("Failed to accept new clients in the TCP server ")
	// 	return numberOfReplica
	// }
	// defer listener.Close()
	//wg1.Wait()
	fmt.Println("Line 271 Fn returning newReplica")

	//go handleConn1(conn)
	//}
	// err2 := listener.Close()
	// if err2 != nil {
	// 	fmt.Println("line 173 error in closing listener in FN newReplica", err2)
	// }
	return 1
}

func portReplica(input []string, mx *sync.Mutex) (string, error) {
	//func identifies a port number and can
	//replicate the code to that port
	fmt.Println("line 282 function portReplica")
	//var wg3 sync.WaitGroup
	fmt.Println("line 284 fn portReplica ", input)
	foundPort := false
	slaveport := ""
	masterPort := ""
	masterExists := false

	var wg2 sync.WaitGroup
	wg2.Add(1)

	for i := 0; i < len(input); i++ {

		if input[i] == "--port" {
			foundPort = true
			//slave port
			fmt.Println("line 313 found slave port ", foundPort)
			slaveport = slaveport + input[i+1]
		}
		if input[i] == "--replicaof" {
			fmt.Println("line 297 --replicaof setting replicainfo to slave ")
			mx.Lock()
			replicainfo.role = "slave"
			mx.Unlock()

			//master port
			masterPort = masterPort + input[i+2]
			for _, port := range replicainfo.ports {
				if port == masterPort {
					fmt.Println("line 306 master Exists", port, masterPort)
					masterExists = true
					return "", nil
				}
			}
		}

		if masterExists {
			fmt.Println("line 372 master exists in portReplica ")
			go replicaPing("localhost", masterPort, mx)
		} else {
			fmt.Println("Line 308 adding a new replica first since one does not exist ")
			newReplica(&wg2, "tcp", "localhost", slaveport, mx)
			wg2.Wait()
=======
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
>>>>>>> 4b8418c4af96120b5f3595e6c0cdf5d3ae2b99ea
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

	//return string1, nil

	// 	//fmt.Println("line 129 ", replicainfo.role)
	// } else if foundPort == false {
	// 	//return "", nil
	// 	continue
	// }
	//}
	if len(slaveport) > 0 {
		wg2.Add(1)
		go replicaStart("localhost", slaveport, &wg2)
		wg2.Wait()
	}

	return slaveport, nil
}

func handleConn1(conn net.Conn, wg *sync.WaitGroup) {

	//defer conn.Close()
	//fmt.Println("Handle connection function")
	inputData := make([]byte, 1024) // buffer to read multiple inputs
	dictionary = make(map[string]string)
	timetracker1 = make(map[string]Data1)

	fmt.Println("Line 366 Fn handle connection ")
	for {

		n, err0r := conn.Read(inputData)
		if err0r != nil {
			fmt.Println("Line 368 Error reading bytes ", err0r)
			wg.Done()
			return
		}
		if n == 0 {
			fmt.Println("line 372 number of bytes in handleConn ")
			wg.Done()
			return
		}
		if n > 0 {
			writeResponse, err := RESPParser(inputData)
			fmt.Println("line 380 write response ", writeResponse)
			if err != nil {
				fmt.Println("Error to run function RESPParser")
			}
			conn.Write([]byte(writeResponse))
		}

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

func replicaPing(host string, port string, mx *sync.Mutex) (string, error) {
	/*
		Function that replicas use to ping master
	*/
	fmt.Println("line 457 logging servers inside map  tracker ", replicainfo.connObject)

	mx.Lock()
	conn := replicainfo.connObject["master0"]
	//conn, err := net.Dial("tcp", host+":"+port)

	replicainfo.ports = append(replicainfo.ports, port)
	mx.Unlock()

	fmt.Println("\n line 444 added a new port in replicaPIng", replicainfo.ports)
	// if err != nil {
	// 	fmt.Println("Falied to open server")
	// }
	//defer conn.Close()
	message := "*1\r\n$4\r\nping\r\n"

	// conn, err := (*Listener).Accept()
	// if err != nil {
	// 	fmt.Println("line 462 could not start connection to server ")
	// }
	defer (*conn).Close()
	(*conn).Write([]byte(message))
	//CRLF := "\r\n"
	return port, nil

	//return "*3\r\n$4\r\nping\r\n" + "$" + strconv.Itoa(len(host)) + CRLF + host + CRLF + "$" + strconv.Itoa(len(port)) + CRLF + port + CRLF, nil
}

func replicaStart(host string, port string, wg *sync.WaitGroup) {
	/* function to create replicas
	solving limitations of new replicas since it dials a master
	*/
	fmt.Println("line 618 creating new replicas")

	var wg5 sync.WaitGroup
	wg5.Add(1)

	listener, err8 := net.Listen("tcp", host+":"+port)

	if err8 != nil {
		fmt.Println("line 624 error in opening server in port", err8)

	}

	conn, err := listener.Accept()

	if err != nil {
		fmt.Println("line 631 error in new connn ")
	}
	defer conn.Close()
	(*wg).Done()
	go handleConn1(conn, &wg5)
	wg5.Wait()

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
		} else if parsedData[i] == " " {
			return "+OK\r\n", nil
		} else if parsedData[i] != " " {
			// parsed array has returned a value then
			len1 := strconv.Itoa(len(parsedData[0]))

			return "$" + len1 + "\r\n" + parsedData[0] + "\r\n", nil
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
		res1, err := executingFunction(input)

		if err != nil {
			fmt.Println("Failed to execute function ")
		}
		return []string{res1}, nil
		//return []string{}, errors.New("Inside ParseArray the passed byte does not follow redis encoding")
	}
	return element, nil
}

func executingFunction(input []byte) (string, error) {
	//implement set function
	i := 0
	for i < len(input) {
		if input[i] == 's' && input[i+1] == 'e' && input[i+2] == 't' {
			// set function implementation
			string1, err := keyValue(input[i+3:])
			if err != nil {
				fmt.Println("Failed to get Key")
			}

			// adding the key value pairs to db
			dictionary[string1[0]] = string1[1]

		} else if input[i] == 'g' && input[i+1] == 'e' && input[i+2] == 't' {
			// implementing get function
			getResult, err := keyValue(input[i+3:])
			if err != nil {
				fmt.Println("Failed to execute get operation")

			}
			key := getResult[0]
			res1 := dictionary[key] // value in the dictionary

			return res1, nil

		}

		i++
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
