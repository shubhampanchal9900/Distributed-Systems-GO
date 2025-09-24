package mr

import (
	"os"
	"strconv"
)

type TaskType int

const (
	// TaskNone means the coordinator has not no task that this worker can start
	TaskNone TaskType = iota

	//TaskMap tells the worker to perform a map task over a single input file
	TaskMap
	// //TaskWait tells the worker to wait for the task to be assigned
	// TaskWait
	//TaskReduce tells the worker to perform a reduce task over the intermediate files
	TaskReduce

	//TaskExit tells the worker to exit
	TaskExit
)

// keyvalue is public big K ...
// Standard pair produced by map and consumed by reduce
type KeyValue struct {
	Key   string
	Value string
}

// request args i.e. the rpc requests when worker asks for a work
type RequestTaskArgs struct{}

type RequestTaskReply struct {
	Type    TaskType // what kind of task is this  (Map/Reduce/EXIT/nONE)
	TaskID  int      // the unique id of the task
	Files   []string // input file to map
	NReduce int      // total number of reduce tasks in the job
	NMap    int      // total number of map tasks in the job

}

// reportTaskArgs is sent by a worker to coordinator when it finishes a task
type ReportTaskArgs struct {
	Type   TaskType // task can be map or reduce so
	TaskID int      // id
	Ok     bool     // finished or not finished
}

// for future
type ReportTaskReply struct{}

func coordinatorSock() string {
	return "var/temp/ds" + strconv.Itoa(os.Getuid())
}
