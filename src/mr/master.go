package mr

import (
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
)

type Master struct {
	// Your definitions here.
	workerNum     int
	completedJobs int
	JobsNum       int
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (m *Master) Register(args *ExampleArgs, reply *ExampleReply) error {
	if args.X != 1 {
		return errors.New("error args")
	}
	m.workerNum += 1
	fmt.Print("Registered work")
	return nil
}

func (m *Master) Finished(args *ExampleArgs, reply *ExampleReply) error {
	if args.X != 1 {
		return errors.New("error args")
	}
	m.completedJobs += 1
	fmt.Print("Finished work")
	return nil
}

// start a thread that listens for RPCs from worker.go
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
func (m *Master) Done() bool {
	if m.completedJobs != m.JobsNum {
		return false
	}
	return true
}

// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.

	m.server()
	return &m
}
