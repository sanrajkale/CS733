# RAFT

This is the Fourth assignment of course CS733: Advanced Distributed Computing - Engineering a Cloud. 
The purpose of this assignment is to understand and implement the RAFT protocol.
We have a set of servers. One of them is made the leader, which communicates with the clients over TCP. 
Leader communicates with the followers through RPC.

# Files

This assignment contains 3 packages.
Structure is as Follows:
	program
		program.go: This file contains raft implementation. It contains Leader side code and follower side code. This file 			also responsible for handling client connection, replying to cleint and making persistence storage of log
		program_test.go This file contains the test case for testing functionality in program.go
	raft
		command.go: This file contains all KV store related function. 
		dataTypes.go: This file contains data structures required
	spawner
		startservers.go: This file contains code to spawn multiple servers. Currently by defualt it spawns 5 server.

# Description

Client can send request to any running server. If server is follower it sends `ERR_REDIRECT` message along with host name 
and port number of the leader. With this information the client can make TCP connection with it. If the server is the leader, it accepts the command, checks if it is not a read request and add it to it shared log.
It then updates the log of the other servers via RPC call, after which it commits if log is replicated majority of servers.
Committed Logs are store on disk for persistence. Currently this program does not handle the recovery from persistent logs. 

The servers can be executed in any order. One will wait for the other to spawn and then connect. The `main()` creates a new `Raft` object initializing it with all the server details.

Following is the description of implemented functions in program

 - `func Init`: It initializes the server, i.e. starts threads for different purposes.
 - `func AcceptConnection`: It accepts an incoming TCP connection request at port 9000 (leader), and spawn handler `ClientListener` for that client.
 - `func ClientListener`: It is used to take input request from a client.
 - `func Evaluator`: It reads from the input channel `input_ch`, processes the request and sends the reply to the output channel `output_ch`.
 - `func DataWriter`: It reads from the output channel `output_ch`, and sends the output to the respective client (if required).
 - `func ConnectToServers`: It is executed to connect each server to every other server.
 - `func AcceptRPC`: Similar to `AcceptConnection`, this function accepts RPC connection request at port 9001 (leader).
 - `func Append`: This appends data in shared log.
 - `func Commit`: Runs the command and commits to the KVStore.
 - `func AppendCaller` : Replicates the log to follower.
 - `func sendAppendRpc` : Sends the log entry/heart beat to single Follower 
 - `func AppendRPC` : This function updates followers log. If Received Log entry is empty, it is treated as heart Beat
 - `func VoteForLeader` accept the vote request from candiate and replies whether voted or not.Its this server is already a leader or candiate , it rejects the vote straight way else it compares its log with received parameters to decide 
 - `func SyncAllLog`:  This Function Syncs log till log index of given in argument.This function sends log entries to follower pointed by NextIndex for that server. If Particular servers log is updated one, it just sends heartbeat to that follower, If log entry gets the majority, it is applied by sending it to Input_ch channel same as AppencCaller Mathod.
 - `func prepareHeartBeat`: This function prepare Heart Beat agruments. If log is having more than two entries , is sends the prevLogIndex and PrevLogTerm of heighest log entry If log has no entry, it sends current term as prevLogTerm
 - `func SendBeat`: This Function sends Regular heart beat to all follower if followers log is in sync with Leaders log. Else this function call SychAllLog function, which in turn sends heart beat or Log Entry as required depending on NextIndex
 - `func SendHeartbeat` : This function invokes SendBeat method. Immediate Heart beats are send after new leader is elected. HeartBeatTimer is reset every time HeartBeat is sent. If Append Log request(AppendCaller() is invoked) is send, Heart Beat timer is reset 
 - `func CallElection` : is responsible for leader election. It send vote request to all the servers and if vote request is positive and in majority , it sets candidate as leader	
 - `func sendVoteRequestRpc` : send the vote request from Candiate to other server
 - `func VoteForLeader` : accept the vote request from candiate and replies whether voted or not. Its this server is already a leader or candiate , it rejects the vote straight way else it compares its log with received parameters to decide 




# Usage

   - Go to “program” directory from command line and run: `go install`
   - Go to “spawner” directory from command line and run:  `go install`
   - Go to “bin” directory then run spawner which will start the servers `./spawner`
   - Go to “program” directory and run for testing the assignment: `go test`

 
 
