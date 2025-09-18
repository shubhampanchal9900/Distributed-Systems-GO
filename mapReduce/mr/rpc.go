package mr

type TaskType int

const (
	// TaskNone means the coordinator has not no task that this worker can start
	TaskNone TaskType = iota

	//TaskMap tells the worker to perform a map task over a single input file
	TaskMap
	//TaskWait tells the worker to wait for the task to be assigned
	TaskWait
	//TaskReduce tells the worker to perform a reduce task over the intermediate files
	TaskReduce

	//TasjkExit tells the worker to exit
	TaskExit
)
