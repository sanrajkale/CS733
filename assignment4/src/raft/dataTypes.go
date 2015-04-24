package raft
import (
		"time"
		"net"
		)

type Lsn uint64 //Log sequence number, unique for all time.

type Value struct {
	Text			string
	ExpiryTime		time.Time
	IsExpTimeInf	bool
	NumBytes		int
	Version			int64
}

type String_Conn struct {
	Text string
	Conn net.Conn
}

type LogEntry interface {
	Lsn() Lsn
	Data() []byte
	Committed() bool
}
type SharedLog interface {
	Init()
	Append(data []byte) (LogEntry, error)
	Commit(sequenceNumber Lsn, conn net.Conn)
}

var KVStore = make(map[string]Value)
var Output_ch = make(chan String_Conn, 10000)
var Connected_ch = make(chan int , 100)
var appendCaller_ch = make ( chan int , 10000)
var AppendHeartbeat = make(chan int,10000)
var CommitHeartbeat= make(chan int,10000)
var ElectionTimer_ch= make(chan int,10000)
var SendImmediateHeartBit = make(chan int,10000)




