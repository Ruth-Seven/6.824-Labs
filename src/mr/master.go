package mr

import (
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

type Master struct {
	// Your definitions here.
	workerNum atomic.Int32

	//map
	files           []string
	mapPendingIndex atomic.Int32
	mapJobsNum      int32
	mapFinished     atomic.Int32

	intermediaKvs KVs
	kvsMu         sync.Mutex
	splited       atomic.Bool
	//reduce
	nReduce            int32
	reduceKvsBuckets   []KVs
	reduceJobsNum      int32
	reduceFinished     atomic.Int32
	reducePendingIndex atomic.Int32

	//Recovery
	waitingJobs        []Task
	maxWaitingDuraiton time.Duration
}

// Your code here -- RPC handlers for the worker to call.
//todo: intermedia file
//todo: resend jobs when works die, crash.go
//todo:  	to a temp file to write reduce res
//'

func (m *Master) Register(args *Args, reply *Reply) error {
	reply.RegisterID = m.workerNum.Add(1)
	fmt.Printf("Registered worker number: %d\n", m.workerNum.Load())
	return nil
}

func (m *Master) Finished(args *Args, reply *Reply) error {
	if args.Job.MrType == Map {
		finished := m.mapFinished.Add(1)
		m.kvsMu.Lock()
		m.intermediaKvs = append(m.intermediaKvs, args.Job.Kvs...)
		m.kvsMu.Unlock()
		m.ReleaseWaitJob(args.Job)
		fmt.Printf("Finished map Jobs: %d, taskNo: %d from worker %d\n", finished, args.Job.TaskNo, args.RegisterID)
		if finished == m.mapJobsNum {
			fmt.Print("Map stage finished.\n")
			m.splitMidData()
			m.splited.Store(true)
		}
	} else if args.Job.MrType == Reduce {
		finished := m.reduceFinished.Add(1)
		m.ReleaseWaitJob(args.Job)
		fmt.Printf("Finished reduce Jobs: %d, taskNo: %d from worker %d\n", finished, args.Job.TaskNo, args.RegisterID)
		if finished == m.reduceJobsNum {
			fmt.Print("Reduce stage finished.\n")
			fmt.Print("All job done.\n")
		}
	}
	return nil
}

func (m *Master) PendJob(args *Args, reply *Reply) error {
	if m.ReIssueJob(args, reply) {
		return nil
	}

	if m.mapPendingIndex.Load() < m.mapJobsNum {
		reply.Job.TaskNo = m.mapPendingIndex.Add(1)
		reply.Job.MrType = Map
		reply.Job.Kvs = append(reply.Job.Kvs, KeyValue{m.files[reply.Job.TaskNo-1], ""})
		m.StoreWaitJob(reply.Job)
		fmt.Printf("Pending map taskNo: %d Job size:%d to worker %d\n", reply.Job.TaskNo, len(reply.Job.Kvs), args.RegisterID)
		return nil
	}

	if !m.splited.Load() {
		m.SendWait(args, reply)
		return nil
	}

	if m.reducePendingIndex.Load() < m.reduceJobsNum {
		reply.Job.TaskNo = m.reducePendingIndex.Add(1)
		reply.Job.MrType = Reduce
		reply.Job.Kvs = append(reply.Job.Kvs, m.reduceKvsBuckets[reply.Job.TaskNo-1]...)
		m.StoreWaitJob(reply.Job)
		fmt.Printf("Pending Reduce taskNo: %d Job size: %d to worker %d\n", reply.Job.TaskNo, len(reply.Job.Kvs), args.RegisterID)
		return nil
	}

	if m.reduceFinished.Load() < m.reduceJobsNum {
		m.SendWait(args, reply)
		return nil
	}

	m.SendClose(args, reply)
	return nil
}

func (m *Master) ReIssueJob(args *Args, reply *Reply) bool {
	for idx, job := range m.waitingJobs {
		if job.SendTime.Add(m.maxWaitingDuraiton).Before(time.Now()) {
			reply.Job = job
			m.waitingJobs[idx].SendTime = time.Now()
			fmt.Printf("ReIssueJob jobno: %d to worker %d\n", job.TaskNo, args.RegisterID)
			return true
		}
	}
	return false
}

func (m *Master) SendClose(args *Args, reply *Reply) {
	reply.Job.Done = true
}

func (m *Master) SendWait(args *Args, reply *Reply) {
	reply.Job.Wait = true
}

func (m *Master) StoreWaitJob(job Task) {
	job.SendTime = time.Now()
	m.waitingJobs = append(m.waitingJobs, job)
}

func (m *Master) ReleaseWaitJob(job Task) {
	for i, wjob := range m.waitingJobs {
		if job.TaskNo == wjob.TaskNo && job.MrType == wjob.MrType {
			m.waitingJobs = append(m.waitingJobs[:i], m.waitingJobs[i+1:]...)
		}
	}
}

func (m *Master) splitMidData() error {
	sort.Sort(m.intermediaKvs)
	for i := 0; i < len(m.intermediaKvs); i++ {
		m.reduceKvsBuckets[m.BucketId(m.intermediaKvs[i].Key)] =
			append(m.reduceKvsBuckets[m.BucketId(m.intermediaKvs[i].Key)], m.intermediaKvs[i])
	}
	for i, data := range m.reduceKvsBuckets {
		fmt.Printf("Split: %d th Data has size %d\n", i, len(data))
	}
	return nil
}

func (m *Master) BucketId(str string) int32 {
	return ihash(str) % m.nReduce
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
	if m.reduceFinished.Load() != m.reduceJobsNum {
		return false
	}
	return true
}

// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeMaster(files []string, nReduce int) *Master {
	err := errors.New("")
	m := Master{}

	m.files = files
	m.nReduce = int32(nReduce)
	m.mapJobsNum = int32(len(files))

	m.reduceKvsBuckets = make([]KVs, nReduce, nReduce)
	m.reduceJobsNum = m.nReduce
	m.maxWaitingDuraiton, err = time.ParseDuration("10s")
	if err != nil {
		log.Fatal("Parse Duration: ", err)
	}
	fmt.Printf("Map jobs: %d Reduce jobs: %d\n", m.mapJobsNum, m.reduceJobsNum)
	m.server()
	return &m
}
