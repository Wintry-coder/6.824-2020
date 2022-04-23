package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "os"
import "io/ioutil"
import "strconv"
import "encoding/json"
import "sort"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

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

type WorkTask struct {
	flag bool
	Files []string
	TaskType int // 1 is map, 2 is reduce
	TaskID int
	// nReduce int
}

func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the master.
	// CallExample()
	for true {
		Files := []string{}
		task := WorkTask{}
		task = CallRPC(0, 0, 0, 0, Files) // 接任务

		if task.flag == false {
			break;
		}

		if task.TaskType == 1 { //map 任务
			intermediate := []KeyValue{}
			file, err := os.Open(task.Files[0])
			if err != nil {
				log.Fatalf("cannot open %v", task.Files[0])
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", task.Files[0])
			}
			file.Close()
			kva := mapf(task.Files[0], string(content))
			intermediate = append(intermediate, kva...)

			imname := "mr-" + strconv.Itoa(task.TaskID) + "-"
			
			tmpfile := []string{}
			ofile := [10](*os.File){}
			for i := 0; i < 10; i++ {
				tmpfile = append(tmpfile, imname + strconv.Itoa(i))
				ofile[i], _ = os.Create(tmpfile[i])
			}
			for i := 0; i < len(intermediate); i++ {
				p := ihash(intermediate[i].Key) % 10
				enc := json.NewEncoder(ofile[p])
   				enc.Encode(&intermediate[i])
			}
			for i := 0; i < 10; i++ {
				ofile[i].Close()
			}
			CallRPC(1, 1, 2, task.TaskID, tmpfile)
		}

		if task.TaskType == 2 { //reduce 任务
			
			intermediate := []KeyValue{}
			var kv KeyValue
			
			for  i := 0; i < len(task.Files); i++ {
				ofile, _ := os.Open(task.Files[i])
				dec := json.NewDecoder(ofile)
		
				for {
					if err := dec.Decode(&kv); err != nil {
						break
					}
					intermediate = append(intermediate, kv)
				}
				ofile.Close()
			}
			sort.Sort(ByKey(intermediate))

			// fmt.Printf("len(intermediate) %v\n", len(intermediate))

			oname := "mr-out-"
			ofile, _ := os.Create(oname + strconv.Itoa(task.TaskID))

			i := 0
			
			for i < len(intermediate) {
				j := i + 1
				for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, intermediate[k].Value)  //将相同Key的value都收集起来
				}
				output := reducef(intermediate[i].Key, values)  //reduce进行处理

				// this is the correct format for each line of Reduce output.
				fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)  //写入

				i = j
			}
			ofile.Close()
			CallRPC(1, 2, 2, task.TaskID, []string{})
		}
	}
}

func CallRPC(option int, TaskType int, TaskState int, 
				TaskID int, Files []string) WorkTask{
	args := Args{}
	args.X = 22
	args.RPCType = option
	args.TaskType = TaskType
	args.TaskState = TaskState
	args.TaskID = TaskID
	args.Files = Files

	reply := Reply{}
	task := WorkTask{}
	call("Master.RPCHandler", &args, &reply)
	if reply.Y != args.X + 1 {
		task.flag = false
		return task
	}
	task.Files = reply.Files

	task.TaskType = reply.TaskType
	task.TaskID = reply.TaskID

	task.flag = true
	return task
}

//
// example function to show how to make an RPC call to the master.
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
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
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
		// log.Fatal("dialing:", err)
		return false
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply) //调用master里的rpc响应函数
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
