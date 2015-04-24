package main 

import (
	"raft"
	//"fmt"
	"log"
	"time"
	"strings"
	"net"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	
)

var waitgrp sync.WaitGroup

var r *Raft

var mutex = &sync.RWMutex{}
var logMutex = &sync.RWMutex{}


type LogEntry struct {
	Term int
	SequenceNumber int
	Command []byte
	IsCommitted bool
}

type ServerConfig struct {
	Id int 				// Id of server. Must be unique
	Hostname string 	// name of host, currently ip is defualt used as localhost
	ClientPort int 		// port at which server listens to client messages.
	LogPort int 		// tcp port for inter-replica protocol messages.
	Client *rpc.Client	// Connection object for the server
}

type ClusterConfig struct {
	//Path string				// Directory for persistent log
	Servers []ServerConfig	// All servers in this cluster
}

type Raft struct {
	Id int
	ClusterConfigV *ClusterConfig  // information of all servers in one place
	Log []LogEntry   				// Logs 
	CurrentTerm int               // Current Term of Server. 
	VotedTerm int                 // Term id for which was voted in last election. 
	VotedFor int                 // Candidate Id that recieved vote in current term form this server, initialised to -1
	CommitIndex int              //index of the highest log entry known to be committed
	LastApplied int              //index of the log entry which is applied to the KV store
	NextIndex []int              // Array of index , each index points to next log entry to be send to respective Follower from leader. 
								// initalised to lenght of log
	MatchIndex []int             // for each server, index of heighest log entry known to be replicated on server, initilaised to 0 
	IsLeader int		     // 1, if the server is the leader, 2 if follower and 0 if candidate
	LeaderId int			// ID of the leader
	File *os.File           // file handle for log storing
}

type AppendRPCArgs struct {    // Structure for arguments to be sent for appending log entries to follower or sending heartbeat
	Term int 				  // Term of the log entry , current term in case of heartbeat
	LeaderId int 	          
	PrevLogIndex int          // Index of entry immediatly preceding new one
	PrevLogTerm int           // term of prevlogIndex 
	Entry LogEntry 			 // log entry to store
	LeaderCommit int        // leader's commit index, used by follower to commit its own log entries
}

type AppendRPCReply struct   // Structure for reply , after sending Apppend entries RPC
{
	NextIndex int      // Next Index of log entries that need to be send in next append RPC
	MatchIndex int     // heighest index of log of follower matching with Leader log
	Reply bool        // true, Log entry is appened else otherwise
	Message string   //TODO
}

type RequestVoteRPCArgs struct   // Strucure for exchabge of vore request by candidate
{
	Term int              // Current term of candiate
	CandidateId int        // Server Id of candiate  
	LastLogTerm int        // Term of last log present in Log entries of candiate
	LastLogIndex int      // Index of last log in log entries of candiate
}

type Log_Conn struct {   // Structur used to pass Log entry and connection to reply after applying command
	Logentry LogEntry 
	Conn net.Conn
}

type CommitI_LogI struct {     // Structure used by follower 
	CommitIndex int 			// CommitIndex of Leader 
	LogIndex int 				// heighest log index which needs to be commited
}

type RPC struct {   // For RPC calling 
}


func (l LogEntry) Lsn() int {
	return l.SequenceNumber
}
func (l LogEntry) Data() []byte {
	return l.Command
}
func (l LogEntry) Committed() bool {
	return l.IsCommitted
}

// ----------------------------------------------



var Append_ch = make(chan Log_Conn, 10000)   // channel for replicating log entries to followers
var Input_ch = make(chan Log_Conn, 10000)   // channel for log entries which are ready to evaluated
var CommitCh =  make(chan CommitI_LogI , 10000)  // channel used by follower, for commiting it log entries



var ElectionTimer *time.Timer
/*
	This function resets the election timer for server. 
	Timer for each each server is set different to avoid election congestion
	This function is called whenever follower is receives heartBeat or append Log request
*/
func (r *Raft) ResetTimer(){
	//fmt.Println("Election TImer Reset")
	if r.Id==0 {
 	ElectionTimer.Reset(time.Millisecond*10000) 	
	}else if r.Id==1 {
 	ElectionTimer.Reset(time.Millisecond*3000)
    }else if r.Id==2 {
 	ElectionTimer.Reset(time.Millisecond*12000)
	}else if r.Id==3 {
 	ElectionTimer.Reset(time.Millisecond*14000)
    }else if r.Id==4 {
 	ElectionTimer.Reset(time.Millisecond*16000)
	}else {
	ElectionTimer.Reset(time.Millisecond*18000)
	}

}

/*
	This function sets the election timer for server. 
	Timer for each each server is set different to avoid election congestion
*/
func (r *Raft) SetElectionTimer() {
	if r.Id==0 {
		ElectionTimer = time.NewTimer(time.Millisecond*8000)
	} else if r.Id==1 {
		ElectionTimer = time.NewTimer(time.Millisecond*2500)
	} else if r.Id==2 {
		ElectionTimer = time.NewTimer(time.Millisecond*8500)
	} else if r.Id==3 {
		ElectionTimer = time.NewTimer(time.Millisecond*9000)		
	} else if r.Id==4 {
		ElectionTimer = time.NewTimer(time.Millisecond*9500)		
	}
}

// This Function waits for electio Timer to get expired, if election timer is expired, 
// it calles election to elect new leader
func Loop(){
	for {
			 <-ElectionTimer.C
			if r.Id == r.LeaderId { 
					//r.ResetTimer()
				}else{
					r.CallElection()							
				}
		}//end of for	
}

/*
func CallElection()	is responsibel for leader election. 
It send vote request to all the servers and if vote request is positive and in majority , it sets candidate as leader	
*/
func (r *Raft) CallElection(){
	
	r.CurrentTerm+=1  // increase the current term by 1 to avoid conflict
	VoteAckcount:=1   // Number of vote received, initialised to 1 as own vote fo candiate is positive
	r.IsLeader = 0   // Set the state of server as candiate
	var VoteCount =make (chan int,(len(r.ClusterConfigV.Servers)-1))
	//fmt.Println("Sending vote requests for:",r.Id)
	
	for _,server := range r.ClusterConfigV.Servers {			
				if server.Id != r.Id{
					go r.sendVoteRequestRpc(server,VoteCount)   					
				}}

	for i:=0;i< len(r.ClusterConfigV.Servers)-1;i++ {
					VoteAckcount  = VoteAckcount+ <- VoteCount 
					// if Candiate gets majoirty, declare candiate as Leader and send immediae heartbeat to followers declaring
					// election of new leader
				if VoteAckcount > (len(r.ClusterConfigV.Servers)/2) && r.IsLeader == 0  { 
					log.Println("New leader is:",r.Id)
					r.IsLeader=1
					r.LeaderId=r.Id
					raft.SendImmediateHeartBit <- 1
					break
					}
				}		
		if r.IsLeader==1{
			// initlised next index to lastlog index, and match index to 0 fro all servers
		for _,server := range r.ClusterConfigV.Servers {
				r.NextIndex[server.Id]=len(r.Log)
				r.MatchIndex[server.Id]=0
				r.ResetTimer()
			}
		}else{ 
			// Is candidate fails to get elected, fall back to follower state and reset timer for reelection 
			r.IsLeader=2
			r.ResetTimer()
		}
}

/*
func sendVoteRequestRpc send the vote request from Candiate to other servers
*/

func (r *Raft) sendVoteRequestRpc(value ServerConfig, VoteCount chan int) error {

	client, err := rpc.Dial("tcp", "localhost:"+strconv.Itoa(value.LogPort))
	log.Println("Dialing vote request rpc from:",r.Id," to:",value.Id)

	 if err != nil {
		log.Print("Error Dialing sendVoteRequestRpc:", err)
		VoteCount<-0
		return err
	 }

	 logLen:= len(r.Log)
	 var lastLogIndex int
	 var lastLogTerm int

	 if logLen >0 {   // if log is not empty, send index and term of last log
	 	lastLogIndex=logLen-1	 	
	 	lastLogTerm = r.Log[lastLogIndex].Term
	 } else {   // if log is empty, send index and term as 0
	 	lastLogIndex=0
	 	lastLogTerm=0
	 }

	 // Prepare argumenst to be sent to follower
	 args:= RequestVoteRPCArgs{r.CurrentTerm,r.Id,lastLogTerm,lastLogIndex,}

	var reply bool  // reply variable will reciece the vote from other server, true is voted, false otherwise
	defer client.Close()
	err1 := client.Call("RPC.VoteForLeader", &args, &reply) 

	if err1 != nil {
		log.Print("Remote Method Invocation Error:Vote Request:", err1)
	}
	if(reply) {  // if reply is positive infrom the candiate 
		//fmt.Println("Received reply of vote request from:",value.Id," for:",r.Id)
		VoteCount <-1	
	}else{
		VoteCount <-0 // if reply is negative infrom the candiate 
		//fmt.Println("Received Negative reply of vote request from:",value.Id," for:",r.Id)
	}
	return nil
}

/*
func VoteForLeader accept the vote request from candiate and replies whether voted or not.
Its this server is already a leader or candiate , it rejects the vote straight way
else it compares its log with received parameters to decide 
*/

func (a *RPC) VoteForLeader(args *RequestVoteRPCArgs,reply *bool) error{
	//r.ResetTimer()
 	//fmt.Println("received Vote request parameter ",(*args).CandidateId," ",(*args).Term," ",(*args).LastLogTerm," ",(*args).LastLogIndex)
 	//if len(r.Log)>1{
 	//	fmt.Println("Vote Request folloer parameter ",r.Id," ", r.CurrentTerm,"  ",r.Log[len(r.Log)-1].Term ," ",len(r.Log)-1)
 	//}
	if r.IsLeader==2 {    // if this server is follower
		//r.ResetTimer() //TODO
		if r.CurrentTerm > args.Term || r.VotedFor >-1 {   // if follower has updated Term or already voted for other candidate in same term , reply nagative
			*reply = false
		} else if r.VotedFor== -1{   // if follower has not voted for anyone in current Term 
			lastIndex:= len(r.Log)  
			if lastIndex > 0 && args.LastLogIndex >0{   // if Candiate log and this server log is not empty. 
				if r.Log[lastIndex-1].Term > args.LastLogTerm {  // and Term of last log in follower is updated than Candidate, reject vote
                      *reply=false
                  }else if r.Log[lastIndex-1].Term == args.LastLogTerm{  // else if Terms of Follower and candidate is same
                  	if (lastIndex-1) >args.LastLogIndex {  // but follower log is more updated, reject vote
                  		*reply = false
                  	} else {
                  			*reply = true   // If last log terms is match and followe log is sync with candiate, vote for candidate
                  		}
                  }else{   // if last log term is not updated and Term does not match, 
                  	 		*reply=true//means follower is lagging behind candiate in log entries, vote for candidate
                  		}
                  	
			} else if lastIndex >args.LastLogIndex {  // either of them is Zero
				*reply = false   // if Follower has entries in Log, its more updated, reject vote
			}else{
					*reply = true  // else Vote for candiate
				}
		}else{
			*reply=false
		}
	}else{
		*reply = false  // This server is already a leader or candiate, reject vote
	}

	if(*reply) {
             r.VotedFor=args.CandidateId   // Set Voted for to candiate Id if this server has voted positive
      }
	/*if(*reply) {
		fmt.Println("Follower ",r.Id," Voted for ",r.VotedFor)
	}else{
		fmt.Println("Follower ",r.Id," rejected vote for ",args.CandidateId)
	}*/
	return nil
}


/*
AppendRPC : 
This function updates followers log.
If Received Log entry is empty, it is treated as heart Beat
*/
func (a *RPC) AppendRPC(args *AppendRPCArgs, reply *AppendRPCReply) error {
	//raft.ElectionTimer_ch <- args.LeaderId   //TODO
	r.ResetTimer()   // Reset timer for election 
	mutex.Lock()  	 
	r.ResetTimer()
    var logIndex int  
    if len(r.Log) > 0 {  // If Log is not emtpy.. Initialised Log intex to last heighest log index
    	logIndex =len(r.Log)-1
    }else{ 
    	logIndex =0  // Else Log index is 0
    }
   //fmt.Println("LogInedx ",logIndex," PrevLogIndex ",args.PrevLogIndex)
	if len(args.Entry.Command)!=0{   // If This request has actual logentry to append, else it is heartbeat. 
		
		r.IsLeader=2  				 // Fall back to Follower state 
		r.LeaderId=args.LeaderId	 // Update to current Leader id 
		r.VotedFor=-1      			 // Election is over, No need to remember whome u voted for. 
									// Thank god... Leader will keep remembering you periodaically :)
    
		   	 if(args.Term < r.CurrentTerm) { // If this logentry has came from Previous Term.. Just Reject it. 
		    	reply.Reply=false
		    } else if (logIndex <args.PrevLogIndex) {   // log lagging behind, 
		    	reply.Reply=false                       // Set appened to false and 
		        reply.NextIndex=logIndex+1              // Set next expected log entry to Heighet log Index +1
		        reply.MatchIndex=-1                 
		        r.CurrentTerm=args.Term	
		    } else if (logIndex > args.PrevLogIndex){    // log is ahead  
		    	 if (r.Log[args.PrevLogIndex].Term != args.PrevLogTerm) {   // If previous log term does matches with leaders Previous log term 
		    	 			reply.Reply=false   
		                    reply.NextIndex=args.PrevLogIndex            // Set expected next log index to previous to do check matching
		                	reply.MatchIndex = -1
		                	r.CurrentTerm=args.Term	
		            } else{       										// Else Terms is matching, overwrite with log with new entry
		            		r.Log[args.PrevLogIndex+1]=args.Entry   
							reply.Reply=true
		                	reply.MatchIndex=args.PrevLogIndex+1        // Match Index is set to added entry 
		                	reply.NextIndex=args.PrevLogIndex+2       // Expected Entry is next log entry after added entry
		                	r.CurrentTerm=args.Term	
		                	//fmt.Println("Calling commit in logIndex>PrevLogIndex")
		                	CommitCh <- CommitI_LogI{args.LeaderCommit,args.PrevLogIndex+1}   // Send Commit index to commitCh to commit log entries, Commit only till newly added aentry
		            }
		    }else if(logIndex == args.PrevLogIndex) {  // log is at same space
		    	if logIndex!=0 && (r.Log[logIndex].Term != args.PrevLogTerm) {   // if log is not emtpy, and previous log tersm is matching
		                    reply.Reply=false  									// Reject the log entry 
		                    reply.NextIndex=args.PrevLogIndex 
		                    reply.MatchIndex = -1
		                    r.CurrentTerm=args.Term	
		                } else if len(r.Log)==0 && args.Entry.SequenceNumber==0{  // If log is empty and Recieved log entry index is 0, Add Entry
		                			r.Log=append(r.Log,args.Entry) 	
		                      		reply.Reply=true
		                      		reply.NextIndex=len(r.Log)    				
		                      		reply.MatchIndex=len(r.Log)-1
		                      		r.CurrentTerm=args.Term	
		                      		//fmt.Println("Calling commit in logIndex=PrevLogIndex")
		                      		CommitCh <- CommitI_LogI{args.LeaderCommit,len(r.Log)-1}
		                }else if len(r.Log)!=args.Entry.SequenceNumber{   // If log is empty and Recieved log entry index is not 0, Missmatch, Reject
		                   		   	//r.Log=append(r.Log,args.Entry)
		                      		reply.Reply=false
		                      		reply.NextIndex=len(r.Log)
		                      		reply.MatchIndex=-1
		                      		r.CurrentTerm=args.Term	
		                      		//fmt.Println("Calling commit in logIndex=PrevLogIndex")
		                      		//CommitCh <- CommitI_LogI{args.LeaderCommit,len(r.Log)-1}
		                }else {											// Previous log is matched , and this is new entry, add it to last of log
		                			r.Log=append(r.Log,args.Entry)
		                      		reply.Reply=true
		                      		reply.NextIndex=len(r.Log)
		                      		reply.MatchIndex=len(r.Log)-1
		                      		r.CurrentTerm=args.Term	
		                      		//fmt.Println("Calling commit in logIndex=PrevLogIndex")
		                      		CommitCh <- CommitI_LogI{args.LeaderCommit,len(r.Log)-1}
		                }
		    }
		   /* if len (args.Entry.Command)!=0{
				fmt.Println("Received append rpc for",r.Id ," From ",args.LeaderId, " Log size is ",logIndex, " == ",args.PrevLogIndex," < ", args.Entry.SequenceNumber ," Commitindex ",r.CommitIndex," < ",args.LeaderCommit, "added ",reply.Reply)
			}*/
	r.ResetTimer()   // This is For precautionaru measure, as system was slow and it was taking more time, leading to expiry of timer
					// Before replying 	
	}else
	{
		/*
		This part is same as above but only without actually aadding entries to log. Next index and match index is updated.
		and CommitCh is feed with commit Index entries
		*/
		//fmt.Println("Heart Beat recieved ",r.Id," ","LogInedx " , len(r.Log)-1," PrevLogIndex ",args.PrevLogIndex)
		//fmt.Println("LogInedx ",logIndex," PrevLogIndex ",args.PrevLogIndex)
		 if(r.CurrentTerm <= args.Term) {  
				r.IsLeader=2
				r.LeaderId=args.LeaderId	
				r.VotedFor=-1
				r.CurrentTerm=args.Term	
				if(logIndex == args.PrevLogIndex && len(r.Log)==0){
					reply.NextIndex=0
					reply.MatchIndex=-1
					//fmt.Println("HeartBeat Recieved logIndex == args.PrevLogIndex && len(r.Log)==0") 
				}else if (logIndex <args.PrevLogIndex){
					reply.NextIndex=logIndex+1
					reply.MatchIndex=-1
					//fmt.Println("HeartBeat Recieved logIndex <args.PrevLogIndex") 
				}else if (logIndex >args.PrevLogIndex){
					if (r.Log[args.PrevLogIndex].Term != args.PrevLogTerm) {
						reply.Reply=false 
		                reply.NextIndex=-1
		                reply.MatchIndex = -1
					}else{
						reply.Reply=true
		                reply.MatchIndex=args.PrevLogIndex
		                reply.NextIndex=args.PrevLogIndex+1
		                CommitCh <- CommitI_LogI{args.LeaderCommit,args.PrevLogIndex+1}
					}
				}else  if(logIndex == args.PrevLogIndex) {
						if logIndex!=0 && (r.Log[logIndex].Term != args.PrevLogTerm) {
							 reply.Reply=false
		                     reply.NextIndex=-1
		                     reply.MatchIndex = -1

		                 }else{
		                 	reply.Reply=true
		                    reply.NextIndex=args.PrevLogIndex+1
		                    reply.MatchIndex=args.PrevLogIndex
		                    CommitCh <- CommitI_LogI{args.LeaderCommit,len(r.Log)-1}
		                 }
					}
			}
	r.ResetTimer()
	}
    mutex.Unlock()
	return nil
}

/*
	This function sends the log entriy to follower through RPC. RPC AppendRPC is called by this function.
	Its feeds AppendAck_ch by 1 if entry is appended to follower and 0 is not appended. It updates Next Index and Match index 
	for particular server. 	
*/


func (r *Raft) sendAppendRpc(value ServerConfig,appendEntry *AppendRPCArgs, AppendAck_ch chan int,isSync bool) {
	//not to send the append entries rpc to the leader itself 
	client, err := rpc.Dial("tcp", "localhost:"+strconv.Itoa(value.LogPort))
	//fmt.Println("Hostname ",value.Hostname)
	//fmt.Println("Sending Append RPCs to:",value.Hostname+":"+strconv.Itoa(value.LogPort))

	 if err != nil {
		log.Print("Error Dialing :", err)
		AppendAck_ch <- 0
		return
	 }

	//defer value.Client.Close()  //TODO
	 defer client.Close()
	//this reply is the ack from the followers receiving the append entries
	var reply AppendRPCReply

	err1 := client.Call("RPC.AppendRPC", appendEntry, &reply)

	if err1 != nil {
		log.Print("Remote Method Invocation Error:Append RPC:", err)
	}
	
	//fmt.Println("RPC reply from:",value.Hostname+":"+strconv.Itoa(value.LogPort)+" is ",reply.Reply)
	if reply.NextIndex!=-1 {
		
		r.NextIndex[value.Id]=reply.NextIndex	
	}	
	if reply.MatchIndex!=-1 {
		r.MatchIndex[value.Id]=reply.MatchIndex
	}	
	if reply.Reply {
		 AppendAck_ch <- value.Id
	}else {
		AppendAck_ch <-	-1
	}
}

/*

	This Function is responsible for Replicationg logs to Follower Servers.
	It waits fot Append_ch channel to get the log entry which needs to be replicated to follower servers.
	Append_ch ensures that all logs are appended in same order.
	This function ensures that Log is replicated on majority servers. 
	After replicationg to Majority servers, This function feeds the log entry to Input_ch to apply command on KV Store
	If log is not commited in first attempt, it updates follower's log with missing entries by calling fun SyncAllLog.
*/


func  AppendCaller() {
		for {
			log_conn := <-Append_ch
			logentry :=log_conn.Logentry
			conn:=log_conn.Conn
			raft.AppendHeartbeat <- 1    // No need to send heartbeat in this cycle, as sending log entires is also treated as heartbeat
			appendAckcount:=1
			syncNeeded := false
			var logentry1 LogEntry
			var args *AppendRPCArgs   // Prepare Arguments, 
			/*if logentry.SequenceNumber >= 1 {	  // if Log has more than 2 entries
				args = &AppendRPCArgs {
					r.CurrentTerm,
					r.LeaderId,
					logentry.SequenceNumber-1,
					r.Log[logentry.SequenceNumber-1].Term,
					logentry,
					r.CommitIndex,
				}
			} else { 
				args = &AppendRPCArgs {    // if Log has only one entry or no entry
					r.CurrentTerm,
					r.LeaderId,
					0,
					r.CurrentTerm,
					logentry,
					r.CommitIndex,
				}
			}*/

			//fmt.Println("Append Recieved ",logentry.SequenceNumber)
				var AppendAck_ch = make (chan int,len(r.ClusterConfigV.Servers)-1)
				for _,server := range r.ClusterConfigV.Servers {			
						if server.Id != r.Id {
							if(logentry.SequenceNumber>r.NextIndex[server.Id]){
									logentry1 = r.Log[r.NextIndex[server.Id]]
									syncNeeded=true
								}else{
									logentry1 = logentry	
								}
								if logentry1.SequenceNumber >= 1 {	
										args = &AppendRPCArgs {
										r.CurrentTerm,
										r.LeaderId,
										logentry1.SequenceNumber-1,
										r.Log[logentry1.SequenceNumber-1].Term,
										logentry1,
										r.CommitIndex,
									}
								} else {
									args = &AppendRPCArgs {
									r.CurrentTerm,
									r.LeaderId,
									0,
									r.CurrentTerm,
									logentry1,
									r.CommitIndex,
								}
							}			
							go r.sendAppendRpc(server,args,AppendAck_ch,false)  // to send Log entry to follower 
						}
					}
					for j:=0;j<len(r.ClusterConfigV.Servers)-1;j++{
							id:=<- AppendAck_ch 
							if(id!=-1 && r.MatchIndex[id]==logentry.SequenceNumber){
								appendAckcount++
							}
							if appendAckcount > len(r.ClusterConfigV.Servers)/2 {   // If we have majority in log , update commit index
								r.CommitIndex=logentry.SequenceNumber
								logentry.IsCommitted=true
							break			
							}
					}
					/*	majorCount:=0
						for _,serverC:= range r.ClusterConfigV.Servers { // Check if log entry is in majority 
							if serverC.Id !=r.Id && r.MatchIndex[serverC.Id] == logentry.SequenceNumber {
								majorityCount++
							}
						}
					*/
					if(logentry.IsCommitted==true){  // If log is committed, write it to log, and send log entry for evaluation on input_ch
						//fmt.Println("Commited ",logentry.SequenceNumber)
						r.Log[logentry.SequenceNumber].IsCommitted=true
						logentry.IsCommitted=true
						r.CommitIndex=logentry.SequenceNumber
						Input_ch <- Log_Conn{logentry, conn}
						r.File.WriteString(strconv.Itoa(logentry.Term)+" "+strconv.Itoa(logentry.SequenceNumber)+" "+strings.TrimSpace(strings.Replace(string(logentry.Command),"\n"," ",-1))+" "+
		" "+strconv.FormatBool(logentry.IsCommitted))
						r.File.WriteString("\t\r\n");
					} else { 
					 			//if syncNeeded==true{  // If Log is not commited, call thsi function to Sync all logs, Logs are sync only till current Logentry, not beyong this even if 
									// Leader log has go more entries added while executing this
									//fmt.Println("Sync call from append")
								syncNeeded=false
								//fmt.Println("Sync Called from Else")
								SyncAllLog(Log_Conn{logentry,conn})		

						}
					if syncNeeded==true{  // If Log is is commited, call thsi function to Sync all logs, Logs are sync only till current Logentry, not beyong this even if 
									// Leader log has go more entries added while executing this
									//fmt.Println("Sync call from append")
						//	fmt.Println("Sync Called from syncNeeded == True")
							SyncAllLog(Log_Conn{logentry,conn})
						}								
				}
}



/*
Function to write back to client connection
*/

func DataWriter() {
	for {
		replych := <-raft.Output_ch
		text := replych.Text
		conn := replych.Conn			// nil for followers as they don't have to send the reply back
		if text != "" && conn != nil {
			conn.Write([]byte(text))
		}
	}
}

/*
	This function evaluates the command.
	Command evaluation function is called as per type of command. 
	If there is error in command msg is sent back to cleint, informing error
*/

func Evaluator() {
	for {
		inputch := <-Input_ch			// Only one control flow is allowed after this point
		
		input := string(inputch.Logentry.Command)
		conn := inputch.Conn
		
		inputs_ := strings.Split(input, "\r\n")
		inputs := strings.Split(inputs_[0], " ")

		if inputs[0] == "get" {
				raft.Get(input, conn)
		} else if (r.LastApplied<inputch.Logentry.SequenceNumber){
				if inputs[0] == "set" && r.LastApplied==inputch.Logentry.SequenceNumber-1 {
					raft.Set(input, conn)
					r.LastApplied=inputch.Logentry.SequenceNumber
				}else if inputs[0] == "getm" && r.LastApplied==inputch.Logentry.SequenceNumber-1 {
					raft.Getm(input, conn)
					r.LastApplied=inputch.Logentry.SequenceNumber
				}else if inputs[0] == "cas" && r.LastApplied==inputch.Logentry.SequenceNumber-1 {
					raft.Cas(input, conn)
					r.LastApplied=inputch.Logentry.SequenceNumber
				} else if inputs[0] == "delete" && r.LastApplied==inputch.Logentry.SequenceNumber-1{
					raft.Delete(input, conn)
					r.LastApplied=inputch.Logentry.SequenceNumber
				}else {
					//fmt.Println("Wrong Command input ",input)
					r.LastApplied=inputch.Logentry.SequenceNumber
					raft.Output_ch <- raft.String_Conn{"ERR_CMD_ERR\r\n", conn}
				}
		}
	}
}


/*
	CommitCaller()
	This function update the commit status in entries of follower log according to leader commit index.
	It waits on CommmitCh channel for new commit index to be appiled on log. THis channel is feed by appendRPC
	After setting it commited, Log entry is send for applying on KV Store though Input_ch channel
	As it is follower log, no need to send reply to client, so connection is nil for input_ch
*/
func CommitCaller(){
	for{
	 commitlog :=<-CommitCh
		for i:=r.CommitIndex+1;i<=commitlog.CommitIndex && i<=commitlog.LogIndex;i++ {
				r.Log[i].IsCommitted=true
				Input_ch <- Log_Conn{r.Log[i], nil}
				r.CommitIndex=r.Log[i].SequenceNumber
			//r.File.WriteString("From Commit Caller "+strconv.Itoa(r.CommitIndex)+" Leader Commit " +strconv.Itoa(commitlog.CommitIndex)+" Log index "+strconv.Itoa(commitlog.LogIndex))
				r.File.WriteString(strconv.Itoa(r.Log[i].Term)+" "+strconv.Itoa(r.Log[i].SequenceNumber)+" "+strings.TrimSpace(strings.Replace(string(r.Log[i].Command),"\n"," ",-1))+" "+" "+strconv.FormatBool(r.Log[i].IsCommitted))
				r.File.WriteString("\t\r\n");
			
		}
	}
}


/*
	This Function Syncs log till log index of Log_Conn.LogEntry.
	This function sends log entries to follower pointed by NextIndex for that server. 
	If Particular servers log is updated one, it just sends heartbeat to that follower, 
	If log entry gets the majority, it is applied by sending it to Input_ch channel same as AppencCaler Mathod. 


*/

func SyncAllLog(log_conn Log_Conn ){
	//fmt.Println("Sync Called")
	for r.IsLeader==1{
		logentry:=log_conn.Logentry
		conn:=log_conn.Conn
		breakVariable:=false
		var args *AppendRPCArgs
		var heartBeat bool
		var logentry1 LogEntry
		//var majority int
		raft.AppendHeartbeat <- 1
		for _,server := range r.ClusterConfigV.Servers {
			if server.Id != r.Id {
				// if Follower not having latast entry and Expected entry index is less or equal to current Log index 
				if r.MatchIndex[server.Id] < (logentry.SequenceNumber) && r.NextIndex[server.Id] <= (logentry.SequenceNumber) { //&& r.MatchIndex[server.Id]!=0 && r.NextIndex[server.Id]!=0
					logentry1=r.Log[r.NextIndex[server.Id]]  // Next to send is entry at Next Index 
					//majority = r.MatchIndex[server.Id]   
					if logentry1.SequenceNumber >= 1 {	
							args = &AppendRPCArgs {
							logentry1.Term,
							r.LeaderId,
							logentry1.SequenceNumber-1,
							r.Log[logentry1.SequenceNumber-1].Term,
							logentry1,
							r.CommitIndex,
							}
					} else {
							args = &AppendRPCArgs {
							logentry1.Term,
							r.LeaderId,
							0,
							logentry1.Term,
							logentry1,
							r.CommitIndex,
						}
				}
					heartBeat = false
				}else {   // Else send normal heart beat to follower
					args = prepareHeartBeat()
					heartBeat = true
				}//  end of if else
					var AppendAck_ch = make (chan int,1)
					if(heartBeat){ 
						var DummyChan = make (chan int,1)
						go r.sendAppendRpc(server,args,DummyChan,false)  // Dont wait on HeartBeat, heart Beat is Send
					}else{
						//fmt.Println("Sending SYnc for server ",server.Id," log  ",logentry1.SequenceNumber)
						go r.sendAppendRpc(server,args,AppendAck_ch,false)  
					}
					if !heartBeat && -1 !=<-AppendAck_ch {  // If Log Entry is send, wait for reply, If log entry is appended to follower
						majorityCount:=0
						for _,serverC:= range r.ClusterConfigV.Servers { // Check if log entry is in majority 
							if serverC.Id !=r.Id && serverC.Id != server.Id && r.MatchIndex[serverC.Id] >= logentry1.SequenceNumber {//&& r.MatchIndex[serverC.Id] != 0 {
								majorityCount++
							}
						}
						// If Log entry is in majority and is not committed yet, commit the log entry and send it to input_ch for evaluation
						if  majorityCount < len(r.ClusterConfigV.Servers)/2 && majorityCount+1 >len(r.ClusterConfigV.Servers)/2{
							r.Log[logentry1.SequenceNumber].IsCommitted=true	
							r.CommitIndex=logentry1.SequenceNumber
							r.File.WriteString(strconv.Itoa(logentry1.Term)+" "+strconv.Itoa(logentry1.SequenceNumber)+" "+strings.TrimSpace(strings.Replace(string(logentry1.Command),"\n"," ",-1))+" "+" "+strconv.FormatBool(logentry1.IsCommitted))
							r.File.WriteString("\t\r\n");
							Input_ch <- Log_Conn{logentry1,conn}
							breakVariable=true
						}else if majorityCount+1 >len(r.ClusterConfigV.Servers)/2 && logentry.SequenceNumber==logentry1.SequenceNumber {
							// if Log was already in majority and now all log is in majority no need to send more log entries
							breakVariable=true
						}
					}// end of IF
				} // outer if	
			}//inner for loop
			if breakVariable{
					break
			}
			majorityCount:=0
			for _,server:= range r.ClusterConfigV.Servers {
						//if i.Id !=raft.ServerId && i!=value && raft.matchIndex[i.Id] >majorityCheck {
				if server.Id !=r.Id  && r.MatchIndex[server.Id] >= logentry.SequenceNumber {
						majorityCount++
					}
				}
				if majorityCount+1 >len(r.ClusterConfigV.Servers)/2  || r.IsLeader!=1{
					break
				}		
	}//outer for loop
	//fmt.Println("Sync Exited")
}


/*
func prepareHeartBeat This function prepare Heart Beat agruments. 
If log is having more than two entries , is sends the prevLogIndex and PrevLogTerm of heighest log entry 
If log has no entry, it sends current term as prevLogTerm
*/
func prepareHeartBeat() *AppendRPCArgs{

						logEntry:=LogEntry{
								r.CurrentTerm,
								len(r.Log),
								make([] byte,0),
								false,
							}								
						var args *AppendRPCArgs
						if len(r.Log) >= 2 {	
								args = &AppendRPCArgs {
										r.CurrentTerm,
										r.LeaderId,
										len(r.Log)-1,
										r.Log[len(r.Log)-2].Term,
										logEntry,
										r.CommitIndex,
									}
						} else if len(r.Log)==1 {
								args = &AppendRPCArgs {
										r.CurrentTerm,
										r.LeaderId,
										0,
										r.Log[len(r.Log)-1].Term,
										logEntry,
										r.CommitIndex,
									}
						} else {
							args = &AppendRPCArgs {
										r.CurrentTerm,
										r.LeaderId,
										0,
										r.CurrentTerm,
										logEntry,
										r.CommitIndex,
									}
						}
						return args
}

/*

	func SendBeat This Function sends Regular heart beat to all follower if followers log is in sync with Leaders log.
	Else this function call SychAllLog function, which in turn sends heart beat or Log ENtry as required depending on NextIndex
*/

func SendBeat(){
	if r.IsLeader==1{
				//	logMutex.Lock()
				//	log_len := len(r.Log)-1
				//	logMutex.Unlock()
					majorityCount:=1
					for _,server:= range r.ClusterConfigV.Servers {
						//if i.Id !=raft.ServerId && i!=value && raft.matchIndex[i.Id] >majorityCheck {
					if server.Id !=r.Id  && r.MatchIndex[server.Id] >= r.CommitIndex && r.MatchIndex[server.Id]!=0{
							majorityCount++
					}
					}
					if majorityCount>len(r.ClusterConfigV.Servers)/2 && majorityCount!=len(r.ClusterConfigV.Servers) && r.CommitIndex != -1 {
						//fmt.Println("Sync will be called ",r.CommitIndex)
						SyncAllLog(Log_Conn{r.Log[r.CommitIndex],nil})
					}else{
						args:=prepareHeartBeat()
						var AppendAck_ch = make(chan int,len(r.ClusterConfigV.Servers)-1)
						for _,server := range r.ClusterConfigV.Servers {			 
							if server.Id == r.Id { continue }				
	 						go r.sendAppendRpc(server,args,AppendAck_ch,false)
						} 
						heartBeatAck:=0
						for j:=0;j<len(r.ClusterConfigV.Servers)-1;j++{
							<- AppendAck_ch 
							heartBeatAck  = heartBeatAck+ 1
							if heartBeatAck > len(r.ClusterConfigV.Servers)/2 { 
								break			
							}
						}
					}
				}//end of if
					
}


var HeartBeatTimer *time.Timer
var StopTimer bool


/*
SendHeartbeat
	This function invokes SendBeat method.
	Immediate Heart beats are send after new leader is elected.
	HeartBeatTimer is reset every time HeartBeat is sent. 
	If Append  Log request is send, Heart Beat timer is reset 
*/
func SendHeartbeat(){
	HeartBeatTimer = time.NewTimer(time.Millisecond*1000)
	for{
		select{
			case <-raft.AppendHeartbeat:
						//fmt.Println("in send SendHeartbeat-Append")
						HeartBeatTimer = time.NewTimer(time.Millisecond*1000)
			case <-raft.SendImmediateHeartBit:
						HeartBeatTimer = time.NewTimer(time.Millisecond*1000)
						SendBeat()

			case <-HeartBeatTimer.C:
						HeartBeatTimer = time.NewTimer(time.Millisecond*1000)
						SendBeat()
						HeartBeatTimer = time.NewTimer(time.Millisecond*1000)
						//fmt.Println("iRegular Heartbeat")
						HeartBeatTimer = time.NewTimer(time.Millisecond*500)
				
		}//end of select
	}//end of for loop
}

/*
func ClientListener , This function listens request from client , if This server is not leader, it replys with
error redirect message to cleint 

If This server is leader, on reciept of command from client, it adds that command to log and sends log on
Append_ch channel for replicationg on follower server. 
With Log it also send the connection obejct , to be used for sending reply of this requestto client after commiting
and applying log entry to KV Store. 
Append to log is done in in Critical section to handle multiole clients at same time 
*/


func (r *Raft) ClientListener(listener net.Conn) {
	command, rem := "", ""
	defer listener.Close()	
	for {			//this is one is outer for
	input := make([]byte, 1000)
	listener.SetDeadline(time.Now().Add(3 * time.Second)) // 3 second timeout
	listener.Read(input)
	input_ := string(Trim(input))
	if len(input_) == 0 { continue }
	if r.Id != r.LeaderId {
		leader := r.ClusterConfigV.Servers[r.LeaderId]

		raft.Output_ch <- raft.String_Conn{"ERR_REDIRECT " + leader.Hostname + " " + strconv.Itoa(leader.ClientPort), listener}
		input = input[:0]
		continue
	}
	command, rem = GetCommand(rem + input_)
	for {
			if command != "" {
			//	if command[0:3] == "get" {					
			//		Input_ch <- Log_Conn{LogEntry{0,0,[]byte(command),true}, listener}
			//	} else {
					commandbytes := []byte(command)		
					logMutex.Lock()			
						var logentry=LogEntry{r.CurrentTerm,len(r.Log),commandbytes,false}
						r.Log=append(r.Log,logentry)
						log.Println("Appennding log ",logentry.SequenceNumber)
						Append_ch <- Log_Conn{logentry, listener}
					logMutex.Unlock()
				//}
			} else { 
				//fmt.Println("I m breaking yaaar. Sorry")
				 break 	} //end of outer if
			command, rem = GetCommand(rem)
		}//end of for
	}//end of outer for
}

/*
This function check the command recieved for its completeness.
*/

func GetCommand(input string) (string, string) {
	inputs := strings.Split(input, "\r\n")
	n1 := len(inputs)
	n := len(inputs[0])
//		abc := input[0:3]
//		log.Printf("**%s--%s--%s--%s-", input, inputs[0], (inputs[0])[1:3], abc)
		
	com, rem := "", ""
	if n >= 3 && (inputs[0][0:3] == "set" || inputs[0][0:3] == "cas") {
		// start of a 2 line command
		if n1 < 3 {						// includes "\r\n"
			return "", input			// if the command is not complete, wait for the rest of the command
		}
		var in = strings.Index(input, "\r\n") + 2
		in += strings.Index(input[in:], "\r\n") + 2
		com = input[:in]
		rem = input[in:]
	} else if (n >= 3 && inputs[0][0:3] == "get") ||
		(n >= 4 && inputs[0][0:4] == "getm") ||
		(n >= 6 && inputs[0][0:6] == "delete") {
		// start of a 1 line command
		if n1 < 2 {						// includes "\r\n"
			return "", input			// if the command is not complete, wait for the rest of the command
		}
		var in = strings.Index(input, "\r\n") + 2
		com = input[:in]
		rem = input[in:]
		
	} else {
		return "", input
	}
	return com, rem
}


func Trim(input []byte) []byte {
	i := 0
	for ; input[i] != 0; i++ {}
	return input[0:i]
}


/* Accepts an incoming connection request from the Client, and spawn a dedicated ClientListener

*/
func (r *Raft) AcceptConnection(port int) {

	tcpAddr, error := net.ResolveTCPAddr("tcp", "localhost:"+strconv.Itoa(port))
	if error != nil {
		log.Print("[Server] Can not resolve address: ", error)
	}
	ln, err := net.Listen(tcpAddr.Network(), tcpAddr.String())
	if err != nil {
		log.Print("[Server] Error in listening: ", err)
	}
	defer ln.Close()
	for {
		// New connection created
		listener, err := ln.Accept()
		if err != nil {
		log.Print("[Server] Error in accepting connection: ", err)
		return
		}
		// Spawn a new listener for this connection
		//fmt.Println("Server accepted new connection, Server ID: ",r.Id)
		go r.ClientListener(listener)
	}
}

var rpcCal=new(RPC) //TODO

/*
This function accept RPC from other servers wanted to communicate via RPC to this server 
*/

func (r *Raft) AcceptRPC(port int) {
// Register this function
	
	rpc.Register(rpcCal)
	//fmt.Println("IN Accept RPC call")

	listener, e := net.Listen("tcp", "localhost:"+strconv.Itoa(port))
	
	if e != nil {
		log.Fatal("[Server] Error in listening:", e)
	}
	defer listener.Close()
	for {
		conn, err := listener.Accept();
		if err != nil {
			log.Fatal("[Server] Error in accepting: ", err)
		} else {

			go rpc.ServeConn(conn)
		}
	}//end of for
//			log.Println("here its end")
}




/*
	This function initliatises the cluster configuration and establishes connection with that 
*/

func ConnectToServers(i int,Servers []ServerConfig) {
	// Check for connection unless the ith server accepts the rpc connection
	var client *rpc.Client = nil
	var err error = nil
	// Polls until the connection is made

	Servers[i] = ServerConfig{
			Id: i,
			Hostname: "Server"+strconv.Itoa(i),
			ClientPort: 9000+2*i,
			LogPort: 9001+2*i,
			Client: client,
	}
	for {
		client, err = rpc.Dial("tcp", "localhost:" + strconv.Itoa(9001+2*i))
		if err == nil {
			//log.Print("Connected to ", strconv.Itoa(9001+2*i))
			break
			}
	}				
}



/*
	This function initilised the server and starts all the go routines for server
*/


func (r *Raft) Init(config *ClusterConfig, thisServerId int) {
	go r.AcceptConnection(r.ClusterConfigV.Servers[thisServerId].ClientPort)   // done
	go r.AcceptRPC(r.ClusterConfigV.Servers[thisServerId].LogPort)	//
	go SendHeartbeat()  // NOT Done TODO
	go Evaluator()	//
	go AppendCaller()
	go CommitCaller()
	go DataWriter()
	r.SetElectionTimer()
	go Loop() //if he is not leader  TODO
}


/*
	Initialise Raft object
*/
func NewRaft(config *ClusterConfig, thisServerId int) (*Raft, error) {
	var raft Raft
	raft.Id = thisServerId
	raft.ClusterConfigV = config
	raft.Log = make([] LogEntry,0)
	raft.VotedTerm=-1
	raft.VotedFor=-1
	raft.CurrentTerm=0
	raft.IsLeader = 2
	raft.NextIndex=make([] int,5)
	raft.MatchIndex=make([] int,5)
	raft.CommitIndex=-1
	raft.LeaderId=-1
	raft.LastApplied = -1 
	filePath:="log_"+strconv.Itoa(thisServerId)+".txt"  // log file
    f, err := os.Create(filePath)
    if(err!=nil) {
        log.Fatal("Error creating log file:",err)
    }
    raft.File = f
	return &raft, nil
}

func main() {
	id, _ := strconv.Atoi(os.Args[1])
	sConfigs := make([]ServerConfig, 5)
	for i:=0; i<5; i++ {		
		if i != id{ // If it is the leader connecting to every other server....?
			go ConnectToServers(i,sConfigs)
		}else{ 
			// Its own detail
				sConfigs[i] = ServerConfig{
				Id: id,
				Hostname: "Server"+strconv.Itoa(i),
				ClientPort: 9000+2*i,
				LogPort: 9001+2*i,
				Client: nil,
			}
		}
	}
	cConfig := ClusterConfig{sConfigs}
	waitgrp.Add(1)
	r, _ = NewRaft(&cConfig, id)
	r.Init(r.ClusterConfigV, r.Id)
	waitgrp.Wait()
	// Wait until some key is press
}
