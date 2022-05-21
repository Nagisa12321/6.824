package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
)

type Coordinator struct {
	// Your definitions here.
	// number of reduce tasks.
	nReduce int
	// number of map tasks.
	nMap int
	// number of the reduce tasks that have done.
	doneReduce int
	// number of the map tasks that have done.
	doneMap int
	// number of the reduce tasks that are started
	startedReduce int
	// number of the map tasks that are started.
	startedMap int
	// files array
	files []string
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) WorkerHello(args *HelloArgs, reply *HelloReply) error {
	fmt.Println(args.Content)
	reply.Content = "hello from coordinator"
	return nil
}

// A worker asked for a job will call this
func (c *Coordinator) WorkerRequest(args *WorkerArgs, reply *WorkerReply) error {
	fmt.Println("worker asked for a job")

	if c.startedMap < c.nMap {
		// some map tasks is not started
		// will start the map task.
		reply.WorkerType = kTypeMap
		reply.MapFile = c.files[c.startedMap]
		c.startedMap++
	} else if c.startedMap == c.nMap && c.doneMap < c.nMap {
		// all map task is started, but not all done.
		// will make the worker waiting.
		reply.WorkerType = kTypeWaiting
		c.startedReduce++
	} else if c.startedMap == c.nMap && c.doneMap == c.nMap {
		// all map is done
		// will start the reduce tasks.
	} else {
		fmt.Println("map count error!")
	}

	return nil
}

func (c *Coordinator) WorkerDone(args *WorkerDoneArgs, reply *WorkerDoneReply) error {
	if args.Success == false {
		fmt.Println("worker failed.")
	} else {
		if args.WorkerType == kTypeMap {
			fmt.Println("a map worker has done a task")
			c.doneMap++
		} else if args.WorkerType == kTypeReduce {
			fmt.Println("a reduce worker has done a task")
			c.doneReduce++
		} else {
			fmt.Println("worker type error")
		}
	}
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
	ret = (c.doneReduce == c.nReduce)

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
	c.nMap = len(files)
	c.nReduce = nReduce
	c.doneMap = 0
	c.doneReduce = 0
	c.startedMap = 0
	c.startedReduce = 0
	c.files = files

	c.server()
	return &c
}
