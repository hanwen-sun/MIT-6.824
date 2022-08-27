package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
)

type Task struct {
	TaskType    int
	MapFileName string
	// ReduceFileName string
	TaskId  int
	NReduce int
	NMap    int // 总的map task数, 即mr-*-n 中*的总数
}

type Coordinator struct {
	// Your definitions here.
	NReduce      int            // 一共分为多少reduce任务;
	NMap         int            // Map任务总数;
	Mapdone      bool           // 判断所有Map任务是否完成;
	Reducedone   bool           // 判断所有Reduce任务是否完成
	MapRecord    map[string]int // 记录所有Map文件的执行情况  0 尚未执行 1 正在执行 2 已处理完
	ReduceRecord map[string]int // 记录所有Reduce文件执行情况
	MapTask      chan Task
	ReduceTask   chan Task
}

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

func (c *Coordinator) AskTask(args *TaskArgs, reply *TaskReply) error {
	if !c.Mapdone { // 分配Map任务
		reply.Reply_ = <-c.MapTask
		fmt.Println("ask task!")
		// fmt.Println(reply.Reply_)
		c.MapRecord[reply.Reply_.MapFileName] = 1
	} else { // 分配Reduce任务

	}

	return nil
}

func (c *Coordinator) DoneTask(args *TaskArgs, reply *TaskReply) error { // 实现可能有问题
	if reply.Reply_.TaskId == 0 {
		c.MapRecord[reply.Reply_.MapFileName] = 2
		for _, v := range c.MapRecord {
			if v != 2 {
				return nil
			}
		}
		fmt.Println("Map Task done!")
		c.Mapdone = true
		c.MakeReduceTasks()
	} else if reply.Reply_.TaskId == 1 {

	}

	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)  // 服务器注册;
	rpc.HandleHTTP() // HTTP作为server和worker的通信协议, HTTP路由注册;
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock() // 自动生成sockname;
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname) // 开启HTTP服务;
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

	return ret
}

var TaskId_ = 0

func (c *Coordinator) MakeMapTasks(files []string, nReduce int) {
	// fmt.Printf("nReduce: %v\n", nReduce)
	for _, filename := range files {
		task_ := Task{
			TaskType:    0,
			MapFileName: filename,
			// ReduceFileName: []string{"nil"},
			TaskId:  TaskId_,
			NReduce: nReduce,
		}
		// fmt.Println(task_)
		// fmt.Println(filename)
		c.MapRecord[filename] = 0
		// fmt.Println("init!")
		c.MapTask <- task_
		// fmt.Println("assign task!")
		TaskId_++ // 这里会有并发的问题;
	}
	// fmt.Println("MapTask make done!")
}

func (c *Coordinator) MakeReduceTasks() { // 定义接口
	// 1. 判断Map任务是否全部完成, 若没有则报错
	// 2. 创造task, 分配文件, mr-*-0
	MapTaskNum := TaskId_ - 1
	TaskId_ = 1
	nreduce_ := c.NReduce
	for i := 0; i < nreduce_; i++ {
		// tmp_name := "mr-*-" + strconv.Itoa(i) // 通配符
		task_ := Task{
			TaskType: 1,
			// ReduceFileName: tmp_name,
			TaskId:  TaskId_,
			NReduce: nreduce_,
			NMap:    MapTaskNum,
		}

		for j := 0; j <= MapTaskNum; j++ {
			tmp_file_name := "mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(j)
			c.ReduceRecord[tmp_file_name] = 0
		}
		c.ReduceTask <- task_
		TaskId_++
	}
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		MapRecord:    make(map[string]int),
		ReduceRecord: make(map[string]int),
		MapTask:      make(chan Task, 100),
		ReduceTask:   make(chan Task, 100),
	}
	c.NReduce = nReduce
	// Your code here.
	fmt.Println(files)
	c.MakeMapTasks(files, nReduce)
	// fmt.Println("Make Map task done!")
	c.server() // 要先注册rpc?

	return &c
}
