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
	nMap                 int         // number of map tasks/input files
	nReduce              int         // number of reduce tasks
	files                []string    // input files
	isMapAssigned        []bool      // if a map task has been assigned
	isMapAccomplished    []bool      // if a map task has been accomplished
	isMapDone            bool        // if all map tasks has been done
	isReduceAssigned     []bool      // if a reduce task has been assigned
	isReduceAccomplished []bool      // is a reduce task has been accomplished
	isDone               bool        // if map-reduce is done
	mapProgress          []time.Time // if map worker has responded
	mux                  sync.Mutex  // lock shared data
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) AssignTask(args *TaskRequesetArgs, reply *TaskReplyArgs) error {
	c.mux.Lock()

	reply.NReduce = c.nReduce
	reply.NMap = c.nMap

	if args.RequestType == 1 {
		if args.TaskType == 0 {
			if args.IsFinish {
				c.isMapAccomplished[args.TaskId] = true
			}
			c.mapProgress[args.TaskId] = time.Now()
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
					if !c.isMapAssigned[i] || time.Now().Sub(c.mapProgress[i]).Seconds() >= 10 {
						reply.X = i
						reply.TaskType = 0
						reply.InputFile = c.files[i]

						c.isMapAssigned[i] = true
						c.mapProgress[i] = time.Now()
						isAssigned = true
						c.mux.Unlock()

						/*go func() {
							log.Printf("enter")
							for !c.isMapAccomplished[i] {
								time.Sleep(10 * time.Second)

								c.mux.Lock()
								if !c.mapHaveProgress[i] {
									// if the map task hasn't responded
									c.isMapAssigned[i] = false
									c.mux.Unlock()
									return
								} else {
									c.mapHaveProgress[i] = false
									c.mux.Unlock()
								}

							}
						}()*/

						// Wait for 10 seconds

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
						c.mux.Unlock()

						go func() {
							// Wait for 10 seconds
							time.Sleep(10 * time.Second)

							c.mux.Lock()
							if !c.isReduceAccomplished[i] {
								// if the map task has not been finished
								c.isReduceAssigned[i] = false
							}
							c.mux.Unlock()
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
	c.mux.Unlock()
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
	c.mux.Lock()
	if c.isDone {
		time.Sleep(5 * time.Second)
		ret = true
	}
	c.mux.Unlock()

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

	c.mapProgress = make([]time.Time, c.nMap)

	c.server()
	return &c
}
