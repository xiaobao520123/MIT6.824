package mr

import (
	"container/list"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"reflect"
	"sync"
	"time"

	"github.com/spf13/cast"
)

var MasterMutex sync.Mutex

type Task struct {
	id    int    // task index
	name  string // task name
	state int
}

type MapTask struct {
	Task
	filename string
}

type ReduceTask struct {
	Task
	filenames []string
}

type Master struct {
	// Your definitions here.
	Filenames []string
	NReduce   int
	state     int // 0=Mapping 1=Reducing

	tasks               map[string]interface{}
	mapTaskQueue        *list.List
	mapTaskDoneQueue    *list.List
	reduceTaskQueue     *list.List
	reduceTaskDoneQueue *list.List
}

// Your code here -- RPC handlers for the worker to call.

func (m *Master) AskTask(args *AskTaskArgs, reply *AskTaskReply) error {
	MasterMutex.Lock()
	defer MasterMutex.Unlock()

	if m.mapTaskQueue.Len() != 0 {
		v := m.mapTaskQueue.Front()
		m.mapTaskQueue.Remove(v)
		task := v.Value.(*MapTask)

		reply.ErrCode = 0
		reply.TaskIndex = task.id
		reply.TaskName = task.name
		reply.TaskType = 1

		reply.Filename = task.filename
		reply.NReduce = m.NReduce
		go m.MonitorTask(task)
	} else if m.reduceTaskQueue.Len() != 0 {
		v := m.reduceTaskQueue.Front()
		m.reduceTaskQueue.Remove(v)
		task := v.Value.(*ReduceTask)

		reply.ErrCode = 0
		reply.TaskIndex = task.id
		reply.TaskName = task.name
		reply.TaskType = 2

		reply.Filenames = task.filenames
		go m.MonitorTask(task)
	} else {
		reply.ErrCode = 1
	}
	return nil
}

func (m *Master) FinishTask(args *FinishTaskArgs, reply *FinishTaskReply) error {
	MasterMutex.Lock()
	defer MasterMutex.Unlock()
	if args.TaskType == 1 {
		name := args.TaskName
		task := m.tasks[name].(*MapTask)

		if task.state == 1 {
			// task already finished!
			reply.ErrCode = 2
			return nil
		}

		task.state = 1
		m.mapTaskDoneQueue.PushBack(task)

		reply.ErrCode = 0
	} else if args.TaskType == 2 {
		name := args.TaskName
		task := m.tasks[name].(*ReduceTask)

		if task.state == 1 {
			// task already finished!
			reply.ErrCode = 2
			return nil
		}

		task.state = 1
		m.reduceTaskDoneQueue.PushBack(task)

		reply.ErrCode = 0
	} else {
		reply.ErrCode = 1
	}
	m.ProcChecker()
	return nil
}

func (m *Master) ProcChecker() {
	if m.state == 0 {
		if m.mapTaskQueue.Len() == 0 && m.mapTaskDoneQueue.Len() == len(m.Filenames) {
			fmt.Println("map procedure finished, now reduce...")

			// Add reduce tasks into queue
			for i := 0; i < m.NReduce; i++ {
				task := &ReduceTask{}
				task.id = i
				task.name = "reduce-" + cast.ToString(i)

				for mapIndex := 0; mapIndex < len(m.Filenames); mapIndex++ {
					filename := "mr-" + cast.ToString(mapIndex) + "-" + cast.ToString(i)
					task.filenames = append(task.filenames, filename)
				}

				task.state = 0

				m.reduceTaskQueue.PushBack(task)
				m.tasks[task.name] = task
			}

			m.state = 1
		}
	} else if m.state == 1 {
		if m.reduceTaskQueue.Len() == 0 && m.reduceTaskDoneQueue.Len() == m.NReduce {
			fmt.Println("reduce procedure finished, exit...")
			m.state = 2
		}
	}
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
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

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false

	MasterMutex.Lock()
	defer MasterMutex.Unlock()
	// Your code here.
	ret = m.state == 2

	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.

	m.tasks = make(map[string]interface{})
	m.Filenames = files
	m.NReduce = nReduce
	m.state = 0
	m.mapTaskQueue = list.New()
	m.mapTaskDoneQueue = list.New()
	m.reduceTaskQueue = list.New()
	m.reduceTaskDoneQueue = list.New()

	// Add map tasks into queue
	for i, filename := range m.Filenames {
		task := &MapTask{}
		task.id = i
		task.name = "map-" + cast.ToString(i)
		task.filename = filename
		task.state = 0

		m.mapTaskQueue.PushBack(task)
		m.tasks[task.name] = task
	}

	m.server()
	return &m
}

//
// monitor tasks
//
func (m *Master) MonitorTask(task interface{}) {
	time.Sleep(time.Second * 10)

	MasterMutex.Lock()
	defer MasterMutex.Unlock()
	if reflect.TypeOf(task) == reflect.TypeOf(&MapTask{}) {
		mapTask := task.(*MapTask)
		if mapTask.state == 0 {
			m.mapTaskQueue.PushBack(mapTask)

			// worker probably crash!
			fmt.Printf("task have not been finished after 10s, go back, task=%v\n", task)
		}
	} else if reflect.TypeOf(task) == reflect.TypeOf(&ReduceTask{}) {
		reduceTask := task.(*ReduceTask)
		if reduceTask.state == 0 {
			m.reduceTaskQueue.PushBack(reduceTask)

			// worker probably crash!
			fmt.Printf("task have not been finished after 10s, go back, task=%v\n", task)
		}
	}
}
