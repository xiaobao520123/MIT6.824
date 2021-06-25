package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"

	"github.com/spf13/cast"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
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

	for {
		// ask for a task
		task, err := CallAskTask()
		if err != nil {
			fmt.Printf("get a new task failed, err: %v\n", err)
			continue
		}
		if task.ErrCode == 1 {
			fmt.Printf("nothing to do yet, sleeping...\n")
			time.Sleep(time.Second)
			continue
		}

		fmt.Printf("got a task, task=%v\n", task)

		if task.TaskType == 1 {
			doMapWork(task, mapf)
		} else if task.TaskType == 2 {
			doReduceWork(task, reducef)
		} else {
			fmt.Printf("task type is invalid, type=%v\n", task.TaskType)
		}
		fmt.Printf("task finished, name=%v, type=%v\n", task.TaskName, task.TaskType)
	}

	// uncomment to send the Example RPC to the master.
	// CallExample()

}

//
// do a map work
//

func doMapWork(task *AskTaskReply, mapf func(string, string) []KeyValue) {
	filename := task.Filename
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	kva := mapf(filename, string(content))

	type myEncoder struct {
		encoder *json.Encoder
		file    *os.File
	}
	ecd := make(map[int]*myEncoder)
	for i := 0; i < task.NReduce; i++ {
		tmpFilename := "mr-" + cast.ToString(i) + "-" + cast.ToString(time.Now().UnixNano())
		tmpFile, err := ioutil.TempFile("", tmpFilename)
		if err != nil {
			log.Fatalf("cannot open %v, err: %v", tmpFilename, err)
		}
		defer tmpFile.Close()
		ecd[i] = &myEncoder{
			encoder: json.NewEncoder(tmpFile),
			file:    tmpFile,
		}
	}

	for _, kv := range kva {
		reduceIndex := ihash(kv.Key) % task.NReduce

		ecd[reduceIndex].encoder.Encode(&kv)
	}

	for i := 0; i < task.NReduce; i++ {
		filename := "mr-" + cast.ToString(task.TaskIndex) + "-" + cast.ToString(i)
		file, err := os.Create(filename)
		if err != nil {
			log.Fatalf("cannot open %v, err: %v", filename, err)
		}
		defer file.Close()
		ecd[i].file.Seek(0, 0)
		io.Copy(file, ecd[i].file)
	}

	err = CallFinishTask(task)
	if err != nil {
		fmt.Printf("failed to tell the master the task finished, task=%v, err: %v\n", task, err)
		return
	}
}

func doReduceWork(task *AskTaskReply, reducef func(string, []string) string) {
	var intermediate []KeyValue
	for _, filename := range task.Filenames {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v, err: %v", file, err)
		}
		defer file.Close()
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
	}

	sort.Sort(ByKey(intermediate))

	oname := "mr-out-" + cast.ToString(task.TaskIndex)
	tmpFilename := oname + cast.ToString(time.Now())
	ofile, _ := ioutil.TempFile("", tmpFilename)

	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
	//
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	ofile.Seek(0, 0)
	destFile, _ := os.Create(oname)
	io.Copy(destFile, ofile)

	destFile.Close()
	ofile.Close()

	err := CallFinishTask(task)
	if err != nil {
		fmt.Printf("failed to tell the master the task finished, task=%v, err: %v\n", task, err)
		return
	}
}

func CallAskTask() (*AskTaskReply, error) {
	args := AskTaskArgs{}

	reply := AskTaskReply{}

	ok := call("Master.AskTask", &args, &reply)
	if !ok {
		return nil, fmt.Errorf("rpc call failed")
	}
	return &reply, nil
}

func CallFinishTask(task *AskTaskReply) error {
	args := FinishTaskArgs{}

	reply := FinishTaskReply{}

	args.TaskName = task.TaskName
	args.TaskType = task.TaskType

	ok := call("Master.FinishTask", &args, &reply)
	if !ok {
		return fmt.Errorf("rpc call failed")
	}

	switch reply.ErrCode {
	case 0:
		{
			return nil
		}
	case 1:
		{
			return fmt.Errorf("unknown task type")
		}
	case 2:
		{
			return fmt.Errorf("task already been finished")
		}
	}
	return nil
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
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
