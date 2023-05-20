package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
	"time"
)

type Status int

const (
	Available Status = iota
	Busy
	Finshed
)

type MapReduceType int

const (
	Map MapReduceType = iota
	Reduce
)

type Task struct {
	TaskNo   int32
	SendTime time.Time
	MrType   MapReduceType
	Kvs      KVs
	Done     bool
	Wait     bool
}

type Args struct {
	Status     Status
	RegisterID int32
	Job        Task
}

type Reply struct {
	RegisterID int32
	Job        Task
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
