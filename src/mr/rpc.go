package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

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

type TaskArgs struct { // 成员名字首字母大写!
	Id int
}

type TaskReply struct {
	//TaskId    int      // 全局唯一的任务名称;
	//MFileName string   // map任务需要的文件名;
	//Mnum      int      // map任务的总数
	//RFileName []string // Reduce任务文件名;
	//Rnum      int      // Reduce任务总数;
	//TaskType  int      // 任务类型; 0 Map, 1 Reduce, 2 Sleep;
	Reply_ Task
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
