package main

import (
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
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
		listener, err = net.Listen("tcp", host+":"+port)

		if err != nil {
			fmt.Println("Line 14 error opening tcp port", err)
			return
		}
		defer listener.Close()

		//add a new replica info to the replica tracker

		newMaster := ReplicaInfo{
			role:               "master",
			port:               port,
			connObject:         &listener,
			master_replied:     "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb",
			master_repl_offset: 0,
			flagMaster:         1,
		}
		replicainfo = append(replicainfo, &newMaster)

	} else {
		listener = *(*replicainfo[0]).connObject
	}

	fmt.Println("line 87 func main ", len(replicainfo), replicainfo[0].role)
	//contact the master
	n1, erro1 := handshake()
	if erro1 != nil {
		fmt.Println("error in handshake ", erro1, n1)
	}

	for {

		conn, err1 := listener.Accept()
		fmt.Println("line 112 ", conn)
		if err1 != nil {
			fmt.Println("Line 116 listener error ", err1)
			//wg.Done()
			return
		}
		defer conn.Close()

		go handleConn(&conn, &wg)

	}

	wg.Wait()
	fmt.Println("line 55 existed function handleconn ", handleFuncCounter)

}

func handleConn(conn *net.Conn, wg *sync.WaitGroup) {
	fmt.Println("line 131 func handleConn -> ")
	handleFuncCounter++
	defer wg.Done()
	// /string1 := ""

	for {

		fmt.Println("line 77 ALIVE")
		inputByte := make([]byte, 300)

		//defer conn.Close()

		n, err2 := (*conn).Read(inputByte)

		if err2 != nil && err2 == io.EOF {
			fmt.Println("Line 67 Finished reading input bytes")

			//wg.Done()
			return
		}

		if n > 0 {
			fmt.Println("Line 67 Finished reading input bytes")

			string2, err3 := RESPParser(inputByte)

			fmt.Println("Line 101 Func handleConn writing output byte ")
			if err3 != nil {
				fmt.Println("line 75 Function Resp Parser failed")
				wg.Done()
				return
			}
			fmt.Println("Line 108 Responding to the string ->  ", string2)

			(*conn).Write([]byte(string2))
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

	flagMaster := 0
	var replicaListener *net.Listener

	n := len(params)
	if n > 0 {
		for i := 0; i < len(params); i++ {
			if params[i] == "--port" && n < 3 {
				fmt.Println("line 386 inside func ParseInput creating master at port -> ", params[i+1])
				var err error
				newPort := params[i+1] // new port

				replicaListener = new(net.Listener)

				*replicaListener, err = net.Listen("tcp", "localhost"+":"+newPort)

				//replicaListener = &listener1

				if err != nil {
					fmt.Println("line 435 opening server at -> ", newPort)
					// wg.Done()
					return flagMaster, nil, err

				}
				mainReplica := new(ReplicaInfo)

				mainReplica = &ReplicaInfo{
					role:               "master",
					port:               params[i+1],
					flagMaster:         1,
					connObject:         replicaListener,
					master_replied:     "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb",
					master_repl_offset: 0,
				}

				//adding master information to replica tracker
				replicainfo = append(replicainfo, mainReplica)
				fmt.Println("line 426 added master to replica tracker ", params[i+1])

				//fmt.Println("line 424 func ParseInput returning a replica")
				//return 1, replicaListener, nil

			}
			if params[i] == "--replicaof" {
				flagMaster = 1
				var err1 error
				masterReplica := new(ReplicaInfo)

				slaveReplica := new(ReplicaInfo)

				master := new(net.Listener)
				slave := new(net.Listener)

				slaveReplica = &ReplicaInfo{
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

				replicainfo = append(replicainfo, slaveReplica)
				fmt.Println("line 500 added new slave replica at port -> ", params[i-1])

				fmt.Println("line 502 master at -> ", params[i+2])

				*master, err1 = net.Listen("tcp", "localhost"+":"+params[i+2])

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

	return flagMaster, replicaListener, nil

}

func handshake() (int, error) {
	/*
		Function to send handshake from Replica

	*/
	fmt.Println("line 541 sending the server pings ", len(replicainfo))
	response := make([]byte, 300)
	for i := 0; i < len(replicainfo); i++ {
		if replicainfo[i].role == "slave" {
			conn, err := net.Dial("tcp", "localhost"+":"+"6379")
			if err != nil {
				fmt.Println("Line 544 failed to dial master ", err)
				return 1, err
			}
			//defer conn.Close()
			conn.Write([]byte("*1\r\n$4\r\nPING\r\n"))
			n, err1 := conn.Read(response)
			if err1 != nil {
				fmt.Println("line 554 error reading response from master ", err1)
			}
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
			fmt.Println("line 587 replica master responding to master ")
			listener := replicainfo[i].connObject
			conn, err := (*listener).Accept()
			defer conn.Close()

			if err != nil {
				fmt.Println("line 591 Failed to open conn master in handshake ", err)
				return 0, nil
			}

			for i := 0; i < 3; i++ {
				resp4 := make([]byte, 50)
				_, err5 := conn.Read(resp4)
				if err5 != nil {
					fmt.Println("line 597 Failed to read bytes received by master ", err5)
					continue

				}
				parsedData, _ := ParseArray(resp4)
				fmt.Println("line 601 master received ", parsedData[0])
				if len(parsedData) >= 1 {
					if parsedData[0] == "PING" {
						fmt.Println("Line 606 parsed master received PING")
						conn.Write([]byte("$4\r\nPONG\r\n"))
						//return 0, nil
					} else if parsedData[0] == "REPLCONF" {
						conn.Write([]byte("+OK\r\n"))
					} else {
						fmt.Println("Line 619 Received information is not part of handleShake ")
						break
						//return 1, nil
					}
				}

			}

		}
	}

	return 0, nil

}
