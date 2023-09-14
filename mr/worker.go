package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// Borrow code from mrsequential.go
// for sorting by key.
type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// mr-main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	for {
		// send an RPC to the coordinator asking for a task
		args := TaskRequesetArgs{}
		reply := TaskReplyArgs{}

		args.RequestType = 0

		if !call("Coordinator.AssignTask", &args, &reply) {
			return
		}
		taskType := reply.TaskType

		log.Printf("get tesk %v: %v", reply.TaskType, reply.X)
		if taskType == 0 {
			// Perform map task, borrow code from mrsequential.go
			filename := reply.InputFile
			mapTask := reply.X
			nReduce := reply.NReduce

			file, err := os.Open(filename)
			if err != nil {
				log.Fatalf("cannot open %v", filename)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", filename)
			}
			file.Close()
			intermediate := mapf(filename, string(content))

			sort.Sort(ByKey(intermediate))

			// output to intermediate files
			i := 0

			nameMap := make(map[string]*os.File)

			currentDir, err := os.Getwd()
			if err != nil {
				log.Fatalf("cannot get current directory, err: %v", err)
				return
			}

			for i < len(intermediate) {
				Y := ihash(intermediate[i].Key) % nReduce
				oname := "mr-" + strconv.Itoa(mapTask) + "-" + strconv.Itoa(Y)

				value, exists := nameMap[oname]
				var ofile_tmp *os.File

				if exists {
					ofile_tmp = value
				} else {
					ofile_tmp, err = ioutil.TempFile(currentDir, oname)
					if err != nil {
						log.Fatalf("cannot create tmp file, err: %v", err)
						return
					}
					defer ofile_tmp.Close()
				}

				enc := json.NewEncoder(ofile_tmp)

				j := i
				for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
					err := enc.Encode(&intermediate[j])
					if err != nil {
						log.Printf("cannot write to %v, err = %v", ofile_tmp, err)
						return
					}
					j++
				}

				nameMap[oname] = ofile_tmp

				i = j
			}

			for key, value := range nameMap {
				destPath := filepath.Join(currentDir, key)
				if _, err := os.Stat(destPath); err == nil {
					if err := os.Remove(value.Name()); err != nil {
						log.Printf("cannot delete tmpFile, err = %v", err)
						return
					}
				} else if os.IsNotExist(err) {
					if err := os.Rename(value.Name(), destPath); err != nil {
						log.Printf("cannot rename %v, err = %v", key, err)
						return
					}
				} else {
					log.Printf("cannot handle tmpFile, err = %v", err)
					return
				}
			}
		} else if taskType == 1 {
			// perform reduce task, borrow code from mrsequential.go
			reduceTask := reply.X

			currentDir, err := os.Getwd()
			if err != nil {
				log.Fatalf("cannot get current directory, err: %v", err)
			}

			/*pattern := filepath.Join(currentDir, "mr-*"+"-"+strconv.Itoa(reduceTask))
			matches, err := filepath.Glob(pattern)
			if err != nil {
				fmt.Println("cannot get file names, err: %v", err)
				return
			}*/

			// TODO
			log.Printf("Start Reading")

			intermediate := []KeyValue{}
			for i := 0; i < reply.NMap; i++ {
				filename := "mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(reduceTask)
				file, err := os.Open(filename)
				if err != nil {
					log.Printf("cannot open %v, err = %v", filename, err)
					continue
				}
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

			oname := "mr-out-" + strconv.Itoa(reduceTask)

			ofile, err := ioutil.TempFile(currentDir, oname)
			if err != nil {
				log.Fatalf("cannot create tmp file, err: %v", err)
			}
			defer ofile.Close()

			// TODO
			log.Printf("Start Processing")

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

			// TODO
			log.Printf("Start Writing to File")

			destPath := filepath.Join(currentDir, oname)
			if _, err := os.Stat(destPath); err == nil {
				if err := os.Remove(ofile.Name()); err != nil {
					log.Printf("cannot delete tmpFile, err = %v", err)
					return
				}
			} else if os.IsNotExist(err) {
				if err := os.Rename(ofile.Name(), destPath); err != nil {
					log.Printf("cannot rename %v, err = %v", oname, err)
					return
				}

			} else {
				log.Printf("err = %v", err)
			}

		} else if taskType == 2 {
			time.Sleep(1 * time.Second)
			continue
		} else {
			return
		}

		args.RequestType = 1
		args.TaskType = taskType
		args.TaskId = reply.X

		// TODO
		log.Printf("finish %v: %v", taskType, args.TaskId)

		// inform the coordinator of the accomplishment of task
		call("Coordinator.AssignTask", &args, &reply)
		time.Sleep(1000 * time.Millisecond)
	}

}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
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

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
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
