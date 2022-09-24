package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}
type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	args := ExampleArgs{}
	args.X = 1
	var file_num int
	for {
		file_name := MapFile{}
		call("Coordinator.Request_map", &args, &file_name)
		if file_name.Finished {
			file_num = file_name.Index
			break
		}

		file, err := os.Open(file_name.File)
		if err != nil {
			log.Fatalf("cannot open %v", file_name.File)
		}

		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", file_name.File)
		}

		file.Close()

		kva := mapf(file_name.File, string(content))

		// var names []string
		var files []os.File
		var encoders []json.Encoder
		for i := 0; i < file_name.Nreduce; i++ {
			oname := fmt.Sprintf("mr-%d-%d", file_name.Index, i)
			ofile, _ := os.Create(oname)
			files = append(files, *ofile)
			encoders = append(encoders, *json.NewEncoder(ofile))
		}

		for _, kv := range kva {
			err := encoders[ihash(kv.Key)%file_name.Nreduce].Encode(&kv)
			if err != nil {
				log.Fatalf("JSON ENCODER ERROR!")
			}
		}

		for i := 0; i < file_name.Nreduce; i++ {
			files[i].Close()
		}
		hb := HeartBeat{}
		hb.CompleteID = file_name.Index
		args := ExampleArgs{}
		call("Coordinator.Maped_signal", &hb, &args)
	}

	for {
		rf := ReduceFile{}
		kva := []KeyValue{}

		call("Coordinator.Request_reduce", &args, &rf)
		if rf.Finished {
			break
		}
		for i := 0; i < file_num; i++ {
			oname := fmt.Sprintf("mr-%d-%d", i, rf.ReduceID)
			ofile, err := os.Open(oname)
			if err != nil {
				log.Fatalf("cannot open %v", err)
			}

			dec := json.NewDecoder(ofile)
			for {
				var kv KeyValue
				if err := dec.Decode(&kv); err != nil {
					break
				}

				kva = append(kva, kv)
			}
			ofile.Close()
		}
		sort.Sort(ByKey(kva))
		mrname := fmt.Sprintf("mr-out-%d", rf.ReduceID)
		mrfile, _ := os.Create(mrname)
		i := 0
		for i < len(kva) {
			j := i + 1
			for j < len(kva) && kva[j].Key == kva[i].Key {
				j++
			}
			values := []string{}
			for k := i; k < j; k++ {
				values = append(values, kva[k].Value)
			}
			output := reducef(kva[i].Key, values)

			fmt.Fprintf(mrfile, "%v %v\n", kva[i].Key, output)

			i = j
		}
		mrfile.Close()
		hb := HeartBeat{}
		hb.CompleteID = rf.ReduceID
		args := ExampleArgs{}
		call("Coordinator.Reduced_signal", &hb, &args)
	}

	// CallExample()

	// call("Coordinator.ReduceIndex")

}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
