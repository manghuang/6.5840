package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

type RequestWrokerIdReq struct {
}

type RequestWrokerIdResponse struct {
	WorkerId int
	Code int
	Msg string
}

type RequestTaskReq struct {
	WorkerId int
}

type RequestTaskResponse struct {
	Status int
	Index int
	Files []string
	NReduce int
	TaskType int
	Code int
	Msg string
}

type TaskResultReq struct {
	WorkerId int
	Index int
	Files []string
	TaskType int
}

type TaskResultResponse struct {
	Status int
	Code int
	Msg string
}

const(
	Code_Success = 1
	Code_Failed = 2

	Msg_Success = "success"
	Msg_Failed = "failed"

	TaskType_Map = 1
	TaskType_Reduce = 2

	Status_Suc = 1
	Status_Fail = 2
	Status_Done = 3
) 

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
