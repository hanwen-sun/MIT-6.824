package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"
)

type Task struct {
	TaskType    int // 3是kill task;
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
	mutex        sync.Mutex //锁
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
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if !c.Mapdone { // 分配Map任务
		flag := false
		for _, k := range c.MapRecord {
			if k == 0 {
				flag = true
			}
		}

		if !flag { // 任务全部分配出去
			reply.Reply_.TaskType = 2 // sleep
			return nil
		}

		// fmt.Println("ask Map task!")
		// fmt.Println(reply.Reply_)
		/*if c.MapRecord[reply.Reply_.MapFileName] == 1 {
			reply.Reply_.TaskType = 2
			return nil
		}*/

		reply.Reply_ = <-c.MapTask
		c.MapRecord[reply.Reply_.MapFileName] = 1
		go c.HandleTimeOut(reply)
	} else { // 分配Reduce任务
		flag := false
		for _, k := range c.ReduceRecord {
			if k == 0 {
				flag = true
			}
		}

		if !flag { // reduce任务全部分配出去;
			reply.Reply_.TaskType = 2 // sleep
			return nil
		}

		// fmt.Println("ask Reduce Task!")
		reply.Reply_ = <-c.ReduceTask // 这里一定要放在前面?
		for i := 0; i <= reply.Reply_.NMap; i++ {
			tmp := "mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(reply.Reply_.TaskId)
			/*if c.ReduceRecord[tmp] == 1 { // 之前已经分配过, 还未撤销
				reply.Reply_.TaskType = 2
				return nil
			}*/

			c.ReduceRecord[tmp] = 1
		}

		go c.HandleTimeOut(reply)
		// c.MapRecord[] = 1
	}

	return nil
}

func (c *Coordinator) HandleTimeOut(reply *TaskReply) error {
	// 休眠10s
	// 加锁
	// 判断分配的任务是否完成, 若没完成, 取消该任务, 将该任务放入channel;
	time.Sleep(time.Second * 10)
	c.mutex.Lock()
	defer c.mutex.Unlock()

	//fmt.Println("go handeletimeout!")
	//fmt.Println(reply)

	TaskType_ := reply.Reply_.TaskType
	if TaskType_ == 0 {
		//fmt.Println("handle map timeout!")
		if c.MapRecord[reply.Reply_.MapFileName] == 1 { // 还未完成;
			c.MapRecord[reply.Reply_.MapFileName] = 0

			//fmt.Println("handle map timeout!")
			c.MapTask <- reply.Reply_
			fmt.Println("handle map timeout!")
		}

		return nil
	}

	if TaskType_ == 1 {
		//fmt.Println("handle Reduce timeout!")
		flag := false
		for i := 0; i <= reply.Reply_.NMap; i++ {
			tmp := "mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(reply.Reply_.TaskId)
			if c.ReduceRecord[tmp] == 1 {
				flag = true
				break
			}
		}

		if flag {
			for i := 0; i <= reply.Reply_.NMap; i++ {
				tmp := "mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(reply.Reply_.TaskId)
				c.ReduceRecord[tmp] = 0
			}

			//fmt.Println("handle reduce time out!")
			c.ReduceTask <- reply.Reply_
			fmt.Println("handle reduce time out!")
		}
		return nil
	}

	return nil
}

func (c *Coordinator) DoneTask(args *TaskReply, reply *TaskArgs) error { // 实现可能有问题
	// fmt.Println("call DoneTask!")
	//fmt.Println(args)
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if args.Reply_.TaskType == 0 {
		// fmt.Println(reply.Reply_)
		c.MapRecord[args.Reply_.MapFileName] = 2
		//for k, v := range c.MapRecord {
		//	fmt.Println(k, v)
		//}

		for _, v := range c.MapRecord {
			if v != 2 {
				//fmt.Println(k, v)
				return nil
			}
		}

		fmt.Println("Map Task all done!")
		c.Mapdone = true
		c.MakeReduceTasks()
	} else if args.Reply_.TaskType == 1 {
		// c.ReduceRecord[reply.Reply_.MapFileName] = 2
		for i := 0; i <= args.Reply_.NMap; i++ {
			tmp := "mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(args.Reply_.TaskId)
			c.ReduceRecord[tmp] = 2
		}

		// fmt.Println(c.ReduceRecord)
		for _, v := range c.ReduceRecord {
			if v != 2 {
				return nil
			}
		}

		fmt.Println("Reduce Task done!")
		c.Reducedone = true
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
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if c.Reducedone {
		ret = true
	}

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

	fmt.Println("Map Task make done!")
	//for k, v := range c.MapRecord {
	//	fmt.Println(k, v)
	//}
	// fmt.Println("MapTask make done!")
}

func (c *Coordinator) MakeReduceTasks() { // 定义接口
	// 1. 判断Map任务是否全部完成, 若没有则报错
	// 2. 创造task, 分配文件, mr-*-0
	MapTaskNum := TaskId_ - 1
	TaskId_ = 0
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
			tmp_file_name := "mr-" + strconv.Itoa(j) + "-" + strconv.Itoa(i)
			c.ReduceRecord[tmp_file_name] = 0
		}
		c.ReduceTask <- task_
		TaskId_++
	}

	fmt.Println("Reduce Task make done!")
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
	// fmt.Println(files)
	c.MakeMapTasks(files, nReduce)
	// fmt.Println("Make Map task done!")
	c.server() // 要先注册rpc?

	return &c
}
