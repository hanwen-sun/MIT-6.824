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

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

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

func HandleMap(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string, reply *TaskReply, args *TaskArgs) error {
	// fmt.Println("Begin Map Task!")
	file, err := os.Open(reply.Reply_.MapFileName)
	intermediate := []KeyValue{}

	if err != nil {
		log.Fatalf("cannot open %v", reply.Reply_.MapFileName)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", reply.Reply_.MapFileName)
	}
	file.Close()
	kva := mapf(reply.Reply_.MapFileName, string(content))
	intermediate = append(intermediate, kva...)
	// 把kva写入json文件, 注意使用ihash判断写入哪个文件

	nReduce_ := reply.Reply_.NReduce
	w_json := make([][]KeyValue, nReduce_)
	for _, kv := range intermediate {
		n := ihash(kv.Key) % nReduce_
		// intername := "mr" + strconv.Itoa(reply.Reply_.TaskId) + strconv.Itoa(n) // mr-m-n
		w_json[n] = append(w_json[n], kv)
	}

	for i := 0; i < nReduce_; i++ {
		interpath := "mr-" + strconv.Itoa(reply.Reply_.TaskId) + "-" + strconv.Itoa(i) + ".json"
		Info_of_intername, err := json.Marshal(w_json[i])
		if err == nil {
			// fmt.Println(string(Info_of_intername))
		} else {
			fmt.Println(err)
			return err
		}

		// fmt.Println(interpath)
		err = ioutil.WriteFile(interpath, Info_of_intername, 0777)
		//fmt.Println("write success!")

		if err != nil {
			fmt.Println("err is:")
			fmt.Println(err)
		}
	}
	// fmt.Println("json write done!")

	// fmt.Println(reply)
	ok_ := call("Coordinator.DoneTask", &reply, &args)
	if !ok_ {
		fmt.Println("Worker Done Map Task Failed!")
	}

	return nil
}

func HandleReduce(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string, reply *TaskReply, args *TaskArgs) error {
	// fmt.Println("Begin Reduce Task!")
	// 1. 把所有文件的内容读入
	// 2  sort
	// 3  处理
	TaskId_ := reply.Reply_.TaskId
	NMap_ := reply.Reply_.NMap

	intermediate := []KeyValue{}
	for i := 0; i <= NMap_; i++ {
		filename := "mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(TaskId_) + ".json"
		jsonFile, err := os.Open(filename)
		if err != nil {
			fmt.Println("error opening json file")
			return err
		}

		intermediate_tmp := []KeyValue{}
		jsonData, err := ioutil.ReadAll(jsonFile)
		if err != nil {
			fmt.Println("error reading json file")
			return err
		}

		json.Unmarshal(jsonData, &intermediate_tmp)

		intermediate = append(intermediate, intermediate_tmp...)

		// intermediate = append(intermediate, intermediate_tmp)
	}
	sort.Sort(ByKey(intermediate))
	oname := "mr-out-" + strconv.Itoa(TaskId_)

	ofile, _ := ioutil.TempFile("", "mr-tmp-*")
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

	oldpath := filepath.Join(ofile.Name())
	os.Rename(oldpath, oname)

	ok_ := call("Coordinator.DoneTask", &reply, &args)
	if !ok_ {
		fmt.Println("Worker Done Reduce Task Failed!")
	}

	return nil
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	// mapf, reducef := loadPlugin(os.Args[1]) // 加载函数;

	for { // 无限循环
		args := TaskArgs{}
		args.Id = 0
		reply := TaskReply{}
		ok := call("Coordinator.AskTask", &args, &reply) // 注意别拼错
		// intermediate := []KeyValue{}
		if !ok {
			fmt.Println("Worker Ask Task Failed!")
		} else {
			// fmt.Println(reply.Reply_)
			if reply.Reply_.TaskType == 0 { // Map任务
				HandleMap(mapf, reducef, &reply, &args)
			} else if reply.Reply_.TaskType == 1 { // Reduce任务
				HandleReduce(mapf, reducef, &reply, &args)
			} else if reply.Reply_.TaskType == 2 { // Sleep
				time.Sleep(time.Millisecond * 10) // sleep 10毫秒;
			} else if reply.Reply_.TaskType == 3 { // kill Task
				os.Exit(1) // 直接退出进程;
			}
		}
	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
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
	sockname := coordinatorSock()            // 获取链接名称;
	c, err := rpc.DialHTTP("unix", sockname) // 先和服务器建立链接;
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply) // 通过call来发起远程调用;
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
