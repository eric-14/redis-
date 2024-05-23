package main

import (
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Data1 struct {
	value      string
	expiryTime string
	timeNow    time.Time
}

// array tracking servers in redis
var replicainfo []*ReplicaInfo

type ReplicaInfo struct {
	role               string
	connected_slaves   int
	master_replied     string
	port               string // ports running servers
	master_repl_offset int
	//Key is the master or replica + connected_slaves[which is the ID of the server]
	connObject *net.Listener // a map object used to track references to servers running
	flagMaster int           // 1 for maste, 0 for slave
	// second_repl_offset   int
	// repl_backlog_active  int
	// repl_backlog_size    int
	// repl_backlog_histlne int
}

var RedisEmptyFile string = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2"

var data Data1
var timetracker1 map[string]Data1

var host string = "localhost"
var port string = "6379"

var dictionary map[string]string
var handleFuncCounter int

func main() {
	var wg sync.WaitGroup
	// var wgParseInput sync.WaitGroup
	var listener net.Listener
	var err error

	dictionary = make(map[string]string)
	timetracker1 = make(map[string]Data1)

	params := os.Args
	lenParams := len(params)
	// len1 := len(RedisEmptyFile)
	// var emptyRedis string

	//get the flag of the return type
	//wgParseInput.Add(1)
	ParseInput(params)
	//wgParseInput.Wait()

	//port = replicainfo[0]

	fmt.Println("line 59 func main length of replica info ->", len(replicainfo))

	wg.Add(1)

	/*
		When running function handle conn we need to run multiple server
		using a refence to listen switches listen from one server to another
		Thus the previous server is replaced rather than letting it remain alive for the next operation

		Solution

		Instead of passing a reference to fn handleConn pass its value
	*/

	for i := 0; i < len(replicainfo); i++ {
		fmt.Println("line 84 inside replica tracker ", replicainfo[i].connObject)
	}

	if len(replicainfo) == 0 {
		//parsed input failed
		//creating master there was no input passed to the server
		listener := new(net.Listener)

		*listener, err = net.Listen("tcp", host+":"+port)

		//monitor the port where the new master server is created
		fmt.Println("line 88 created the first server at port -> ", port)
		if err != nil {
			fmt.Println("Line 14 error opening tcp port", err)
			return
		}
		defer (*listener).Close()

		//add a new replica info to the replica tracker

		newMaster := ReplicaInfo{
			role:               "master",
			port:               port,
			connObject:         listener,
			master_replied:     "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb",
			master_repl_offset: 0,
			flagMaster:         1,
		}
		replicainfo = append(replicainfo, &newMaster)

	} else {
		//creating listerne
		fmt.Println("line 119 parse Input created the main object  ")
		listener = *(*replicainfo[0]).connObject
	}

	fmt.Println("line 87 func main ", len(replicainfo), replicainfo[0].role, replicainfo[0].port)
	//contact the master
	var connMaster *net.Conn
	var erro1 error

	if lenParams > 2 {
		fmt.Println("line 116 params passed to server ")
		connMaster, erro1 = handshake()
	}

	if erro1 != nil {
		fmt.Println("error in handshake ", erro1, connMaster)
	}

	/*
		Evaluating the hand shake return

		If hand shake is 3 -- means the synchronization of redis replica with master is complete

		Send file
	*/
	//fmt.Println(" Inside main function line 134 return type of handhsake")

	if connMaster != nil {
		// sending redis file to the master
		fmt.Println("line 138 main function sending Redis File")
		sendRedisFile(connMaster)
	}
	//fmt.Println("line 123 length of replica info after handshake ", len(replicainfo), *(replicainfo[0]).connObject, replicainfo[0].role, replicainfo[0].port)

	listener = *(replicainfo[0]).connObject

	for {
		//fmt.Println("line 117 ")
		conn, err1 := listener.Accept()
		// fmt.Println("line 127 ", conn)
		if err1 != nil {
			fmt.Println("Line 129 listener error ", err1)
			//wg.Done()h
			return
		}
		defer conn.Close()

		go handleConn(&conn, &wg)

	}

	wg.Wait()
	fmt.Println("line 55 existed function handleconn ", handleFuncCounter)

}

func handleConn(conn *net.Conn, wg *sync.WaitGroup) {
	fmt.Println("line 169 FUNCTION HANDLECONN -> ")
	handleFuncCounter++
	defer wg.Done()
	// /string1 := ""

	for {

		//fmt.Println("line 77 ALIVE")
		inputByte := make([]byte, 100)

		//defer conn.Close()

		n, err2 := (*conn).Read(inputByte)

		if err2 != nil && err2 != io.EOF {
			// check if error is not nil
			//check if error is not equal to end of file
			fmt.Println("Line 184 Finished reading input bytes")

			//wg.Done()
			//return
			continue
		}

		if n > 0 {
			fmt.Println("Line 194 inished reading input bytes ", inputByte)

			string2, err3 := RESPParser(inputByte)

			fmt.Println("Line 198 inside handleCOnn RESPParser return value ", string2)
			if err3 != nil {
				fmt.Println("line 75 Function Resp Parser failed")
				wg.Done()
				return
			}

			//fmt.Println("Line 108 Responding to the string ->  ", string2)

			(*conn).Write([]byte(string2))
			fmt.Println("Line 208 sent bytes PONG to replica  ->  ", string2)
		}
		// if n == 0 {
		// 	fmt.Println("line 86 continue")
		// 	continue
		// }

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
		fmt.Println("line 105 parsedData", i, parsedData[i])
		if parsedData[i] == "ECHO" {
			response = response + string(parsedData[i+1])
		} else if parsedData[i] == "set" || parsedData[i] == "SET" {
			fmt.Println("line 217 adding timetracker fn", len(parsedData))
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
			fmt.Println("line 266 responding to PING with PONG")
			return "$4\r\nPONG\r\n", nil
		} else if parsedData[i] == "INFO" {
			//returning replication information about the server
			//fmt.Println("line 213 found INFO")
			flagReplica := 0 // get information on master service

			//get information on slave service
			if parsedData[i+1] == "command" {
				return "", nil
			}
			if len(parsedData) >= 1 && len(replicainfo) > 0 {
				//len replicainfo is used to track the number of replicas
				//hence its used to validate the number of replica
				//if there is a parameter to INFO flag then get the slaves
				flagReplica = 1
			}
			fmt.Println("line 236 flagReplica  -> ", flagReplica)
			fmt.Println("line 236 length of replicainfo --> ", len(replicainfo))

			dest, nil := ReplicaInformation(len(replicainfo) - 1)

			return dest, nil

		}
		// else if parsedData[i] == "--replicaof" {
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
		if input1[i] == '$' || input1[i] == '+' {

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
		fmt.Println("Inside function parsed Array len", string(rune(input[1])))
		len1 := string(rune(input[1]))
		arrayLen, _ := strconv.ParseInt(len1, 10, 64)
		//number of items in the array

		//pos 2 -- \r
		//pos 3 -- \n
		//j := 4

		for i := 0; i < int(arrayLen); i++ {
			element1, err := ParseString(input[3:], i)

			if err != nil {
				fmt.Println("Failed to parse string ")
			}

			element = append(element, element1)
			fmt.Println("line 206 string returned by function ", element)

		}
	}
	return element, nil
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

func executingFunction(fn int, key string, value string) (string, error) {

	if fn == 0 {
		fmt.Println("line 296 assigning values ", key, value)
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

func ReplicaInformation(flagIndex int) (string, error) {

	if flagIndex == 0 {

	}
	fmt.Println("line 374 printing the role ", replicainfo[flagIndex].role)
	string1 := "role:" + replicainfo[flagIndex].role
	string2 := "master_replid:" + replicainfo[flagIndex].master_replied
	string4 := "master_repl_offset:" + strconv.Itoa(replicainfo[flagIndex].master_repl_offset)
	//$" + strconv.Itoa(len(string1)) + "\r\n" + string1 + "\r\n" + "$" + strconv.Itoa(len(string2)) + "\r\n" + string2 + "\r\n" +
	string3 := "$" + strconv.Itoa(len(string1+string2+string4)+2) + "\r\n" + string1 + "," + string2 + "," + string4 + "\r\n"
	//fmt.Println("line 379 ", string3)
	return string3, nil

}

func ParseInput(params []string) (int, *net.Listener, error) {
	//Function to parse input from the os arguments
	//defer wg.Done()
	fmt.Println(" Line 453 code running Parse Input ", params)
	flagMaster := 0
	var replicaListener *net.Listener

	n := len(params)
	if n > 0 {
		for i := 0; i < len(params); i++ {
			if params[i] == "--port" && n < 4 {
				fmt.Println("line 386 inside func ParseInput creating master at port -> ", params[i+1])
				var err error
				newPort := params[i+1] // new port

				replicaListener = new(net.Listener)

				*replicaListener, err = net.Listen("tcp", "localhost"+":"+newPort)
				fmt.Println("line 489 port address is -> ", newPort)
				//replicaListener = &listener1

				if err != nil {
					fmt.Println("line 435 opening server at -> ", newPort)
					// wg.Done()
					return flagMaster, nil, err

				}
				mainReplica := new(ReplicaInfo)

				mainReplica = &ReplicaInfo{
					role:               "master", //change it to master
					port:               params[i+1],
					flagMaster:         1,
					connObject:         replicaListener,
					master_replied:     "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb",
					master_repl_offset: 0,
				}
				fmt.Println("line 487 adding a new master at port -> ", params[i+1])

				//adding master information to replica tracker
				replicainfo = append(replicainfo, mainReplica)
				fmt.Println("line 512 added master to replica tracker ", params[i+1], mainReplica.port, mainReplica)

				//fmt.Println("line 424 func ParseInput returning a replica")
				//return 1, replicaListener, nil

			}
			if params[i] == "--replicaof" {
				flagMaster = 1
				var err1 error
				masterReplica := new(ReplicaInfo)

				//slaveReplica := new(ReplicaInfo)

				master := new(net.Listener)
				slave := new(net.Listener)
				slave1, _ := net.Listen("tcp", "localhost"+":"+params[i-1])

				slave = &slave1

				slaveReplica := ReplicaInfo{
					role:               "slave",
					port:               params[i-1],
					flagMaster:         1,
					connObject:         slave,
					master_replied:     "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb",
					master_repl_offset: 2,
				}

				// adding master information to replica tracker
				// overwrite master information
				//

				replicainfo = append(replicainfo, &slaveReplica)
				fmt.Println("line 500 added new slave replica at port -> ", params[i-1])

				//fmt.Println("line 502 master at -> ", params[i+2])
				masterPort := params[i+1]
				substrings := strings.Split(masterPort, " ")
				fmt.Println("Master port is ", substrings[0], substrings[1])

				*master, err1 = net.Listen("tcp", substrings[0]+":"+substrings[1])

				if err1 != nil {
					fmt.Println("line 477 error creating localhost ", err1)
					// wg.Done()
					return 0, nil, err1
				}

				masterReplica = &ReplicaInfo{
					role:               "master",
					port:               params[i+2],
					connObject:         master,
					flagMaster:         0,
					master_replied:     "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb",
					master_repl_offset: 1,
				}
				//adding slave information to the replica server tracker

				replicainfo = append(replicainfo, masterReplica)

				//replicainfo[0] = mainReplica
				// wg.Done()
				return flagMaster, nil, nil
			}
		}

	} else if n == 0 {
		//there are no params parsed to function
		// wg.Done()
		return flagMaster, nil, nil
	}
	// wg.Done()
	fmt.Println("line 595 activating replica")

	/*
		From the test paramters it requires data to be sent via the intial replica
		This has been achieved by using func accept on creation of master object to maintainn
		connection to the replica
	*/
	(*replicaListener).Accept()

	//conn1.Write([]byte("+PONG\r\n"))

	return flagMaster, replicaListener, nil

}

func handshake() (*net.Conn, error) {
	/*Function to send handshake from Replica	*/
	handShakeCounter := 0
	fmt.Println("line 599 sending the server pings ", len(replicainfo))
	response := make([]byte, 100)
	for i := 0; i < len(replicainfo); i++ {

		if replicainfo[i].role == "slave" {
			//conn, err := net.Dial("tcp", "localhost"+":"+"6379")
			// if err != nil {
			// 	fmt.Println("Line 544 failed to dial master ", err)
			// 	return nil, err
			// }
			// //defer conn.Close()
			// conn.Write([]byte("*1\r\n$4\r\nPING\r\n"))
			// n, err1 := conn.Read(response)
			// if err1 != nil {
			// 	fmt.Println("line 554 error reading response from master ", err1)
			// }
			conn, _ := (*replicainfo[i].connObject).Accept()

			resp1 := make([]byte, 300)
			n, _ := conn.Read(resp1)

			if n > 0 {
				resp1 := string(response)

				//fmt.Println("line 558 master responding to string -> ", resp1)

				fmt.Println("line 561 received PONG command ", resp1)
				_, err2 := conn.Write([]byte("*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$4\r\n6380\r\n"))
				if err2 != nil {
					fmt.Println("line 562 Erorr in wrting replconf --> ", err2)
				}

			}

			resp2 := make([]byte, 100)
			n2, _ := conn.Read(resp2)
			fmt.Println("line 569 2nd response -> ", resp2)
			if n2 > 0 {
				conn.Write([]byte("*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n"))
				fmt.Println("Line 565 responding with second REPLCONF")
			}

			resp3 := make([]byte, 20)
			n3, _ := conn.Read(resp3)
			fmt.Println("line 579 3nd response -> ", resp3)
			if n3 > 0 {
				conn.Write([]byte("*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n"))
				fmt.Println("Line 582 responding with second REPLCONF")
			}

		} else if replicainfo[i].role == "master" {
			// handshake on the master side
			fmt.Println("line 651 replica master responding to master ", len(replicainfo), i, replicainfo[i].connObject)

			listener12 := *(replicainfo[i].connObject)

			conn89, err14 := listener12.Accept()
			//net.Dial("tcp", "localhost"+":"+replicainfo[i].port)
			//
			//conn89.Write([]byte("+OK\r\n"))
			if err14 != nil {
				fmt.Println("line 657 Failed to open conn master in handshake ", err14)
				return nil, err14
			}
			defer conn89.Close()
			//i := 0; i <= 4; i++
			for i := 0; i < 5; i++ {
				fmt.Println("Line 666 inside for loop ")
				resp6 := make([]byte, 300)

				n1, err5 := conn89.Read(resp6)

				// if err5 != io.EOF {
				// 	fmt.Println("Line 671 reading input EOF error ", err5, n1)
				// 	continue
				// }
				//&& err5 != io.EOF
				if err5 != nil && err5 != io.EOF {
					//when error is not nil and not equal to end of File
					fmt.Println("line 676 Failed to read bytes received by master ", err5)
					break
				}
				//conn89.Write([]byte("+PONG\r\n"))
				if n1 < 0 {
					fmt.Println("Line 679 function handshake returns less than 0 bytes ", n1)
					//return nil, nil
					continue

				}
				parsedData, _ := ParseArray(resp6)
				fmt.Println("line 688 master received ", parsedData)

				if len(parsedData) >= 1 {
					if parsedData[0] == "PING" {
						fmt.Println("Line 682 parsed master received PING")
						conn89.Write([]byte("+PONG\r\n"))
						//conn89.Write([]byte("$4\r\nPONG\r\n"))
						handShakeCounter++

						//return 0, nil
					} else if parsedData[0] == "REPLCONF" {
						conn89.Write([]byte("+OK\r\n"))
						handShakeCounter++

					} else if parsedData[0] == "PSYNC" {
						d, _ := hex.DecodeString(RedisEmptyFile)
						len1 := len(d)

						emptyRedis := "$" + strconv.Itoa(len1) + "\r\n" + string(d)
						// *3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n
						conn89.Write([]byte("+FULLRESYNC 8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb 0\r\n"))
						fmt.Println("Line 698 PSYNC complete returning. Returning from handshake ")

						conn89.Write([]byte(emptyRedis))
						//(*listener).Close()
						//conn.Close()
						return &conn89, nil

					} else {
						fmt.Println("Line 706 Received information is not part of hand Shake ")

						return nil, nil
					}
					if handShakeCounter == 5 {
						fmt.Println("line 711 hand shake complete ")
						return nil, nil
					}

				}

			}

		}
	}

	return nil, nil

}

func sendRedisFile(conn *net.Conn) (int, error) {
	//sending Redis file to the replica
	print("len replica is ", len(replicainfo))

	for i := 0; i < len(replicainfo); i++ {
		if replicainfo[i].role == "master" {

			//listener23 := (replicainfo[i].connObject)
			fmt.Println("line 705 ", conn)
			// conn, err1 := (*listener23).Accept()
			// fmt.Println("line 702 replica master ", conn, err1)
			// if err1 != nil {
			// 	fmt.Println("line 700 failed to write file from master")
			// 	return -1, nil
			// }
			//defer conn.Close()
			//writing empty redis file to replicas from master
			//fmt.Println(" line 708 sending Empty Redis Files from master ", emptyRedis)
			(*conn).Write([]byte("+OK\r\n"))
			emptyRedis := "+OK\r\n"
			(*conn).Write([]byte(emptyRedis))
			return 1, nil
		}
	}
	return 0, nil

}
