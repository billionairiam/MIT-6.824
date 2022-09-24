package mr

import (
	"container/list"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	Reduce_num int
	File_names []string
	sync.Mutex
	MapIndex     int
	ReduceIndex  int
	Fault_names  list.List
	Reduce_names list.List
	Fault_reduce map[int]bool
	RequestIDC   map[int]bool
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) time_stamp(handleID int) {
	time.Sleep(time.Second * 10)
	c.Lock()
	if !c.RequestIDC[handleID] {
		c.Fault_names.PushBack(handleID)
	}
	c.Unlock()
}

func (c *Coordinator) Request_map(args *ExampleArgs, file *MapFile) error {
	c.Lock()
	if c.MapIndex < len(c.File_names) {
		file.Index = c.MapIndex
		file.File = c.File_names[file.Index]
		file.Finished = false
		c.RequestIDC[c.MapIndex] = false
		go c.time_stamp(c.MapIndex)
		file.Nreduce = c.Reduce_num
	} else if c.Fault_names.Len() != 0 {
		file.Index = c.Fault_names.Front().Value.(int)
		c.Fault_names.Remove(c.Fault_names.Front())
		go c.time_stamp(file.Index)
	} else {
		file.Index = len(c.File_names)
		file.Finished = true
	}
	c.MapIndex++
	c.Unlock()
	return nil
}

func (c *Coordinator) Maped_signal(hb *HeartBeat, args *ExampleArgs) error {
	c.Lock()
	c.RequestIDC[hb.CompleteID] = true
	c.Unlock()
	return nil
}

func (c *Coordinator) Reduced_signal(hb *HeartBeat, args *ExampleArgs) error {
	c.Lock()
	c.Fault_reduce[hb.CompleteID] = true
	c.Unlock()
	return nil
}

func (c *Coordinator) _time_stamp(handleID int) {
	c.Lock()
	time.Sleep(time.Second * 10)
	if !c.Fault_reduce[handleID] {
		c.Reduce_names.PushBack(handleID)
	}
	c.Unlock()
}

func (c *Coordinator) Request_reduce(args *ExampleArgs, file *ReduceFile) error {
	c.Lock()
	if c.ReduceIndex < c.Reduce_num {
		file.ReduceID = c.ReduceIndex
		c.Fault_reduce[c.ReduceIndex] = false
		go c._time_stamp(c.ReduceIndex)
	} else if len(c.Fault_reduce) > 0 {
		file.ReduceID = c.Reduce_names.Front().Value.(int)
		c.Reduce_names.Remove(c.Reduce_names.Front())
		go c._time_stamp(file.ReduceID)
	} else {
		file.Finished = true
	}
	c.ReduceIndex++
	c.Unlock()
	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	c.Lock()
	if c.ReduceIndex > c.Reduce_num {
		return true
	}
	c.Unlock()
	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.Reduce_num = nReduce
	c.File_names = files
	c.MapIndex = 0
	c.ReduceIndex = 0
	c.server()
	return &c
}
