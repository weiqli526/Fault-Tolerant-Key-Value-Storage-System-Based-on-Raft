package mr

import (
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
	nMap                 int        // number of map tasks/input files
	nReduce              int        // number of reduce tasks
	files                []string   // input files
	isMapAssigned        []bool     // if a map task has been assigned
	isMapAccomplished    []bool     // if a map task has been accomplished
	isMapDone            bool       // if all map tasks has been done
	isReduceAssigned     []bool     // if a reduce task has been assigned
	isReduceAccomplished []bool     // is a reduce task has been accomplished
	isDone               bool       // if map-reduce is done
	mux                  sync.Mutex // lock shared data
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) AssignTask(args *TaskRequesetArgs, reply *TaskReplyArgs) error {
	c.mux.Lock()
	defer c.mux.Unlock()
	reply.NReduce = c.nReduce
	reply.NMap = c.nMap

	if args.RequestType == 1 {
		if args.TaskType == 0 {
			c.isMapAccomplished[args.TaskId] = true
		} else {
			c.isReduceAccomplished[args.TaskId] = true
		}
	} else {
		if !c.isMapDone {
			// find a map task
			isUnaccomplished := false
			isAssigned := false
			for i := 0; i < c.nMap; i++ {
				if !c.isMapAccomplished[i] {
					isUnaccomplished = true
					if !c.isMapAssigned[i] {
						reply.X = i
						reply.TaskType = 0
						reply.InputFile = c.files[i]

						c.isMapAssigned[i] = true
						isAssigned = true

						// start a new goroutine to asynchronously wait for the worker to finish
						go func() {
							// Wait for 10 seconds
							time.Sleep(1000 * time.Second)

							if !c.isMapAccomplished[i] {
								// if the map task has not been finished
								c.isMapAssigned[i] = false
							}
						}()
						return nil
					}
				}
			}
			if !isUnaccomplished {
				c.isMapDone = true
			} else {
				if !isAssigned {
					// wait
					reply.TaskType = 2
				}
			}
		}
		if c.isMapDone && !c.isDone {
			// find a reduce task
			isUnaccomplished := false
			isAssigned := false
			for i := 0; i < c.nReduce; i++ {
				if !c.isReduceAccomplished[i] {
					isUnaccomplished = true
					if !c.isReduceAssigned[i] {
						isAssigned = true
						reply.X = i
						reply.TaskType = 1

						c.isReduceAssigned[i] = true

						// start a new goroutine to asynchronously wait for the worker to finish
						go func() {
							// Wait for 10 seconds
							time.Sleep(10 * time.Second)

							if !c.isReduceAccomplished[i] {
								// if the map task has not been finished
								c.isReduceAssigned[i] = false
							}
						}()
						return nil
					}
				}
			}
			if !isUnaccomplished {
				c.isDone = true
			} else {
				if !isAssigned {
					// wait
					reply.TaskType = 2
				}
			}

		}
		if c.isDone {
			// assign a exit pseudo-task
			reply.TaskType = 3
		}
	}
	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
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

// mr-main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	if c.isDone {
		time.Sleep(5 * time.Second)
		ret = true
	}

	return ret
}

// create a Coordinator.
// mr-main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.

	// pack up coordinator
	c.nMap = len(files)
	c.nReduce = nReduce
	c.files = files

	c.isMapAssigned = make([]bool, c.nMap)
	c.isReduceAssigned = make([]bool, c.nReduce)
	c.isMapAccomplished = make([]bool, c.nMap)
	c.isReduceAccomplished = make([]bool, c.nReduce)

	c.isMapDone = true

	c.server()
	return &c
}
