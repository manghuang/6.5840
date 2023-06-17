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
	maxWorkerId int
	lock1 sync.Mutex

	mapFiles []string
	mapFileCount int
	mapResult []int

	reduceFiles [][]string
	reduceFileCount int
	reduceResult []int

	reduceResultFiles []string

	done bool
	lock2 sync.Mutex
}

const(
	Result_Init = 1
	Result_Prepare_Done = 2
	Result_Doing = 3
	Result_Done = 4
)

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}


func (c *Coordinator) RequestWrokerId(args *RequestWrokerIdReq, reply *RequestWrokerIdResponse) error {
	c.lock1.Lock()
	defer c.lock1.Unlock()
	reply.WorkerId = c.maxWorkerId
	c.maxWorkerId += 1
	reply.Code = Code_Success
	reply.Msg = Msg_Success
	return nil
}


func (c *Coordinator) RequestTask(args *RequestTaskReq, reply *RequestTaskResponse) error {
	if args.WorkerId >= c.maxWorkerId{
		log.Printf("illegal workerId, %v\n", args.WorkerId)
		reply.Code = Code_Failed
		reply.Msg = Msg_Failed
		return nil
	}
	c.lock2.Lock()
	defer c.lock2.Unlock()
	for idx, result := range c.mapResult{
		if result != Result_Init{
			continue
		}
		reply.Status = Status_Suc
		reply.Index = idx
		reply.NReduce = c.reduceFileCount
		reply.Files = []string{c.mapFiles[idx]}
		reply.TaskType = TaskType_Map
		reply.Code = Code_Success
		reply.Msg = Msg_Success
		c.mapResult[idx] = Result_Doing
		timer := time.NewTimer(1 *  time.Second)
		go func(){
			<- timer.C
			if c.mapResult[idx] == Result_Doing{
				c.mapResult[idx] = Result_Init
			}
		}()
		return nil
	}
	for idx, result := range c.reduceResult{
		if result != Result_Prepare_Done{
			continue
		}
		reply.Status = Status_Suc
		reply.Index = idx
		reply.NReduce = c.reduceFileCount
		reply.Files = c.reduceFiles[idx]
		reply.TaskType = TaskType_Reduce
		reply.Code = Code_Success
		reply.Msg = Msg_Success
		c.reduceResult[idx] = Result_Doing
		timer := time.NewTimer(2 *  time.Second)
		go func(){
			<- timer.C
			if c.reduceResult[idx] == Result_Doing{
				c.reduceResult[idx] = Result_Prepare_Done
			}
		}()
		return nil
	}
	reply.Status = Status_Fail
	if c.done{
		reply.Status = Status_Done
	}
	reply.Code = Code_Success
	reply.Msg = Msg_Success
	return nil
}

func (c *Coordinator) TaskResult(args *TaskResultReq, reply *TaskResultResponse) error {
	if args.WorkerId >= c.maxWorkerId{
		log.Printf("illegal workerId, %v\n", args.WorkerId)
		reply.Code = Code_Failed
		reply.Msg = Msg_Failed
		return nil
	}
	c.lock2.Lock()
	defer c.lock2.Unlock()
	if args.TaskType == TaskType_Map{
		if c.mapResult[args.Index] != Result_Done{
			c.mapResult[args.Index] = Result_Done
			for idx, _ := range c.reduceFiles{
				c.reduceFiles[idx] = append(c.reduceFiles[idx], args.Files[idx])
				if len(c.reduceFiles[idx]) == c.mapFileCount{
					// log.Printf("map done, idx:%v\n", idx)
					c.reduceResult[idx] = Result_Prepare_Done
				}
			}
		}
	}else{
		if c.reduceResult[args.Index] != Result_Done{
			c.reduceResult[args.Index] = Result_Done
			c.reduceResultFiles[args.Index] = args.Files[0]
			c.done = true
			for _, result := range c.reduceResult{
				if result != Result_Done{
					c.done = false
					break
				}
			}
		}
	}
	reply.Status = Status_Suc
	if c.done{
		reply.Status = Status_Done
	}
	reply.Code = Code_Success
	reply.Msg = Msg_Success
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
	ret = c.done

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
	c.maxWorkerId = 0
	c.lock1 = sync.Mutex{}

	c.mapFiles = files
	c.mapFileCount = len(files)
	c.mapResult = make([]int, len(files))
	for idx, _ := range c.mapResult{
		c.mapResult[idx] = Result_Init
	}

	c.reduceFiles = make([][]string, nReduce)
	for idx, _ := range c.reduceFiles{
		c.reduceFiles[idx] = make([]string, 0)
	}
	c.reduceFileCount = nReduce
	c.reduceResult = make([]int, nReduce)
	for idx, _ := range c.reduceResult{
		c.reduceResult[idx] = Result_Init
	}
	c.reduceResultFiles =  make([]string, nReduce)

	c.done = false
	c.lock2 = sync.Mutex{}

	log.Println("MakeCoordinator init...")
	c.server()
	return &c
}
