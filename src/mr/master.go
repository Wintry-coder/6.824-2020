package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "time"
import "fmt"

type MapTask struct {
	File string
	State int // 0 is un-progress , 1 is in-progress, 2 is completed
	Time int64
}

type ReduceTask struct {
	Files []string
	State int // 0 is un-progress , 1 is in-progress, 2 is completed
	Time int64
}

type Master struct {
	// Your definitions here.
	MapJob []MapTask
	ReduceJob []ReduceTask
	isMapDone  bool
	isReduceDone  bool
	nReduce int
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	fmt.Printf("send reply %v\n", reply)
	return nil
}

func (m *Master) RPCHandler(args *Args, reply *Reply) error {
	reply.Y = args.X + 1
	if args.RPCType == 0 { // 此时是接任务
		if m.isMapDone == false {
			isMapDone := true
			for i := 0; i < len(m.MapJob); i++ {
				if m.MapJob[i].State == 0 {
					reply.Files = append(reply.Files, m.MapJob[i].File)
					reply.TaskType = 1
					reply.TaskID =  i

					m.MapJob[i].Time = time.Now().Unix()
					m.MapJob[i].State = 1 // mark it in-progress
					return nil
				}
				if m.MapJob[i].State != 2 {
					isMapDone = false
				}				
			}
			if isMapDone {
				m.isMapDone = true
			}
		} else {
			if m.isReduceDone == false {
				isReduceDone := true
				for i := 0; i < len(m.ReduceJob); i++ {
					if m.ReduceJob[i].State == 0 {
						reply.Files = m.ReduceJob[i].Files
						reply.TaskType = 2
						reply.TaskID =  i

						m.ReduceJob[i].Time = time.Now().Unix()
						m.ReduceJob[i].State = 1 // mark it in-progress
						return nil
					}
					if m.ReduceJob[i].State != 2 {
						isReduceDone = false
					}
				}
				if isReduceDone {
					m.isReduceDone = true
				}
			}
		}
		return nil
	}
	
	if args.TaskType == 1 {
		m.MapJob[args.TaskID].State = 2

		for i := 0; i < len(args.Files); i++ {
			m.ReduceJob[i].Files = append(m.ReduceJob[i].Files, args.Files[i])
		}
	}
	if args.TaskType == 2 {
		m.ReduceJob[args.TaskID].State = 2
	}
	
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
	if m.isMapDone && m.isReduceDone {
		ret = true
	}
	if ret == false {
		nowtime := time.Now().Unix()
		if m.isMapDone == false {
			for i := 0; i < len(m.MapJob); i++ {
				if m.MapJob[i].State == 1 && nowtime - m.MapJob[i].Time >= 10 {
					m.MapJob[i].State = 0
				}
			}
		}
		if m.isReduceDone == false {
			for i := 0; i < len(m.ReduceJob); i++ {
				if m.ReduceJob[i].State == 1 && nowtime - m.ReduceJob[i].Time >= 10 {
					m.ReduceJob[i].State = 0
				}
			}
		}
	}

	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}
	m.isMapDone = true
	m.isReduceDone = true
	m.nReduce = nReduce
	if len(files) > 0 {
		m.isMapDone = false
		m.isReduceDone = false
	}
	
	for i := 0; i < len(files); i++ {
		InitTask := MapTask{files[i], 0, 0}
		m.MapJob = append(m.MapJob, InitTask)
	}

	for i := 0; i < nReduce; i++ {
		InitTask := ReduceTask{}
		m.ReduceJob = append(m.ReduceJob, InitTask)
	}
	

	m.server()
	return &m
}
