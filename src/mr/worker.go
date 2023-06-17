package mr

import (
	"bufio"
	"fmt"
	"hash/fnv"
	"io"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strings"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	// 请求workerid
	args := RequestWrokerIdReq{}
	reply := RequestWrokerIdResponse{}
	ok := call("Coordinator.RequestWrokerId", &args, &reply)
	fmt.Printf("%v   args:%v  reply:%v\n", "RequestWrokerId", args, reply)
	if !ok || reply.Code != Code_Success{
		log.Fatalf("RequestWrokerId err")
	}
	wokerId := reply.WorkerId

	for{
		// 请求任务
		args := RequestTaskReq{
			WorkerId: wokerId,
		}
		reply := RequestTaskResponse{}
		ok = call("Coordinator.RequestTask", &args, &reply)
		fmt.Printf("%v %v   args:%v  reply:%v\n", wokerId, "RequestTask", args, reply)
		if !ok || reply.Code != Code_Success{
			log.Fatalf("RequestTask err")
		}
		if reply.Status == Status_Done{
			log.Printf("Worker %v exit\n", wokerId)
			return
		}else if reply.Status == Status_Fail{
			time.Sleep(time.Duration(1)*time.Second)
			continue
		}

		taskResultFiles := make([]string, 0)
		// 处理任务
		if reply.TaskType == TaskType_Map{
			taskResultFiles = doMap(mapf, reply, wokerId)
		}else{
			taskResultFiles = doReduce(reducef, reply)
		}

		// 上传处理结果
		args1 := TaskResultReq{
			WorkerId: wokerId,
			Index: reply.Index,
			Files: taskResultFiles,
			TaskType: reply.TaskType,
		}
		reply1 := TaskResultResponse{}
		ok = call("Coordinator.TaskResult", &args1, &reply1)
		fmt.Printf("%v %v   args:%v  reply:%v\n", wokerId, "Result", args1, reply1)
		if !ok || reply1.Code != Code_Success{
			log.Fatalf("TaskResult err")
		}
		if reply1.Status == Status_Done{
			log.Printf("Worker %v exit\n", wokerId)
			return
		}
	}


	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}


func doMap(mapf func(string, string) []KeyValue, reply RequestTaskResponse, workerId int)[]string{
	taskResultFiles := make([]string, 0)
	intermediate := []KeyValue{}
	for _, filename := range reply.Files {
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
		intermediate = append(intermediate, kva...)
	}
	intermediateMap := make(map[int][]KeyValue)
	for i:=0; i < reply.NReduce; i++{
		intermediateMap[i] = make([]KeyValue, 0)
	}
	for _, KeyValue := range intermediate{
		tmp := ihash(KeyValue.Key) % reply.NReduce
		intermediateMap[tmp] = append(intermediateMap[tmp], KeyValue)
	}
	for key, intermediate := range intermediateMap{
		// path := fmt.Sprintf("./mr-tmp-workerid-%d", workerId)
		// _, err:= PathExistsAndCreate(path)
		// if err != nil{
		// 	log.Fatalf("PathExistsAndCreate err, info:%v", err)
		// }

		
		oname := fmt.Sprintf("mr-tmp-%d-%d-%d", workerId, reply.Index, key)
		ofile, err := os.Create(oname)
		if err != nil{
			log.Fatalf("Create err, file:%v, info:%v", oname, err)
		}
		for _, KeyValue := range intermediate{
			_, err := fmt.Fprintf(ofile, "%v %v\n", KeyValue.Key,  KeyValue.Value)
			if err != nil{
				log.Fatalf("Fprintf err, file:%v, info:%v", oname, err)
			}
		}
		ofile.Sync()
		ofile.Close()
		taskResultFiles = append(taskResultFiles, oname)
	}
	sort.Strings(taskResultFiles)
	return taskResultFiles
}


// func PathExistsAndCreate(path string)(bool, error){
//     _ , err :=os.Stat(path)
//     if err == nil{//文件或者目录存在
// 		// fmt.Printf("stat suucee, mode:%v\n", info.Mode())
//     	return true, nil
//     }
// 	if os.IsNotExist(err){
// 		err := os.Mkdir(path, 0775)
// 		if err != nil{
// 			return false, err
// 		}
// 		// info , err =os.Stat(path)
// 		// fmt.Printf("Mkdir suucee, mode:%v\n", info.Mode())
// 		syscall.Umask(0)
// 		err = os.Chmod(path, 0775)
// 		if err != nil{
// 			return false, err
// 		}
// 		// info , err =os.Stat(path)
// 		// fmt.Printf("Chmod suucee, mode:%v\n", info.Mode())
//     	return true, nil
//     }
//     return false, err
// }



func doReduce(reducef func(string, []string) string, reply RequestTaskResponse)[]string{
	taskResultFiles := make([]string, 0)
	intermediate := []KeyValue{}
	for _, filename := range reply.Files {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v, err:%v", filename, err)
		}
		defer file.Close()
		// strs := strings.Split(string(content), "\n")
		// fmt.Printf("%+v\n", len(strs))

		read := bufio.NewReader(file)
		for{
			b, _, err := read.ReadLine()
			if err == io.EOF {
				break
			}else if err != nil {
				log.Fatalf("cannot readLine %v", filename)
			}
			vlas := strings.Split(string(b), " ")
			if len(vlas) != 2{
				log.Fatalf("illegal tmp file, %v, %v", filename, len(vlas))
			}
			intermediate = append(intermediate, KeyValue{
				Key: vlas[0],
				Value: vlas[1],
			})
		}
		
	}
	sort.Sort(ByKey(intermediate))
	oname := fmt.Sprintf("mr-out-%d", reply.Index)
	ofile, _ := os.Create(oname)

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
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}
	ofile.Close()
	taskResultFiles = append(taskResultFiles, oname)
	return taskResultFiles
}

//
// example function to show how to make an RPC call to the coordinator.
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
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	// fmt.Printf("%v args:%v\n", rpcname, args)
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		// fmt.Printf("%v reply:%v\n", rpcname, reply)
		return true
	}

	fmt.Println(err)
	return false
}
