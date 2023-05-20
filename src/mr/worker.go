package mr

import (
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"strconv"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type KVs []KeyValue

func (kvs KVs) Len() int           { return len(kvs) }
func (kvs KVs) Swap(i, j int)      { kvs[i], kvs[j] = kvs[j], kvs[i] }
func (kvs KVs) Less(i, j int) bool { return kvs[i].Key < kvs[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int32 {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int32(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	reply := CallRegister()
	workerID := reply.RegisterID
	for true {
		args := Args{RegisterID: workerID}
		reply := CallGiveMeJob(args)
		if reply.Job.Done {
			fmt.Printf("worker exit, workID: %d\n", workerID)
			os.Exit(0)
		}
		if reply.Job.Wait {
			time.Sleep(5 * time.Millisecond)
			continue
		}
		taskNo := reply.Job.TaskNo
		mrType := reply.Job.MrType
		kvs := reply.Job.Kvs
		resKvs := KVs{}
		if mrType == Map {
			for _, kv := range kvs {
				file, err := os.Open(kv.Key)
				if err != nil {
					log.Fatalf("cannot open %v", kv.Key)
				}
				content, err := ioutil.ReadAll(file)
				if err != nil {
					log.Fatalf("cannot read %v", kv.Key)
				}
				file.Close()
				resKvs = append(resKvs, mapf(kv.Key, string(content))...)
			}
		} else if mrType == Reduce {
			groupedKvs := map[string][]string{}
			for _, kv := range kvs {
				groupedKvs[kv.Key] = append(groupedKvs[kv.Key], kv.Value)
			}
			for key, value := range groupedKvs {
				resKvs = append(resKvs, KeyValue{key, reducef(key, value)})
			}
			oname := "mr-out-" + strconv.Itoa(int(taskNo))
			ofile, _ := os.Create(oname)
			for _, kv := range resKvs {
				fmt.Fprintf(ofile, "%v %v\n", kv.Key, kv.Value)
			}
			ofile.Close()
		}

		args.Job = Task{TaskNo: taskNo, MrType: mrType, Kvs: resKvs}
		CallFinished(args)
	}
}

func CallRegister() Reply {
	args := Args{Status: Available}
	reply := Reply{}
	call("Master.Register", &args, &reply)
	return reply
}

func CallGiveMeJob(args Args) Reply {
	args.Status = Available
	reply := Reply{}
	call("Master.PendJob", &args, &reply)
	return reply
}

func CallFinished(args Args) Reply {
	reply := Reply{}
	call("Master.Finished", &args, &reply)
	return reply
}

// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.

func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
		// os.Exit(0)
		// return false
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err != nil {
		// log.Fatal("Call:", err)
		// os.Exit(0)
		// return false
	}

	// fmt.Println(err)
	return true
}
