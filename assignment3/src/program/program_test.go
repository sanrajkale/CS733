package main

//sudo gedit /etc/pam.d/common-session
//sudo gedit /etc/security/limits.conf


import (
		"fmt"
		"log"
		"net"
		"testing"
		"strconv"
)

func TestMain(t *testing.T) {
	// Initializing the server
//	log.Print("Creating Server...")
//	go AcceptConnection()
//	time.Sleep(time.Millisecond * 1)
}


type TestCase struct {
	input		string		// the input command
	output		string		// the expected output
	expectReply	bool		// true if a reply from the server is expected for the input
}

var wait_ch chan int


// This channel is used to know if all the clients have finished their execution
var end_ch chan int

// SpawnClient is spawned for every client passing the id and the testcases it needs to check
func SpawnClient(t *testing.T, id int, testCases []TestCase) {

	// Make the connection
	tcpAddr, err := net.ResolveTCPAddr("tcp", "localhost:9002")
	conn, err := net.DialTCP("tcp", nil, tcpAddr)
	if err != nil {
		log.Print("Error in dialing: ", err)
		return
	}
	defer conn.Close()
	

	
	// Execute the testcases
	for i:=0; i<len(testCases); i++ {
		// Now send data
		input := testCases[i].input
		exp_output := testCases[i].output
		expectReply := testCases[i].expectReply

		conn.Write([]byte(input))
//		time.Sleep(750 * time.Millisecond)
		if !expectReply {
			continue
		}
		reply := make([]byte, 1000)
		conn.Read(reply) 
		//fmt.Println("Got reply ", string(reply))	
//		log.Print("[Client]:",string(reply))
			if exp_output != "" { reply = reply[0:len(exp_output)] }
			// if expected output is "", then don't check
			if exp_output!="" && string(reply) != exp_output {
				t.Error(fmt.Sprintf("Input: %q, Expected Output: %q, Actual Output: %q", input, exp_output, string(reply)))
			}
	}

	// Notify that the process has ended
	end_ch <- id
}

// ClientSpawner spawns n concurrent clients for executing the given testcases. It ends when all of the clients are finished.
func ClientSpawner(t *testing.T, testCases []TestCase, n int) {
	end_ch = make(chan int, n)
	// {input, expected output, reply expected}
	
	for i := 0; i<n; i++ {
		go SpawnClient(t, i, testCases)
	}
	ended := 0
	
	for ended < n {
		<-end_ch
		ended++
	}
}

func TestCase1(t *testing.T) {
	// Number of concurrent clients
	var n = 2
	// ---------- set the values of different keys -----------
	var testCases = []TestCase {
		{"set alpha 0 10\r\nI am ALPHA\r\n", "", true},
		{"set beta 0 9 noreply\r\nI am BETA\r\n", "", false},
		{"set gamma 0 10 noreply\r\nI am GAMMA\r\n", "", false},
		{"set theta 0 10\r\nI am THETA\r\n", "", true},
	}
	ClientSpawner(t, testCases, n)

	// ---------- get theta ----------------------------------
	testCases = []TestCase {
		{"get theta\r\n", "VALUE 10\r\nI am THETA\r\n", true},
	}
	ClientSpawner(t, testCases, n)
	
	ClientSpawner(t, testCases, 10)

	// ---------- get the changed value -----------------------
	testCases = []TestCase {
		{"get gamma\r\n", "VALUE 13\r\nI am BETA now\r\n", true},
		{"getm gamma\r\n", "VALUE "+strconv.Itoa(n+1), true},
	}
	//ClientSpawner(t, testCases, n)
}


