package mr

/*
cooedinator reposibilities
	-keep authoritative state of all map and reduce tasks
	-hand out tasks to workers on demand in the form of RequestTask RPC
	-Mark tasks done on ReportTask RPC
	-Detect and reschedule tasks if a worker "vanishes" (timeout after 10s)
	-Transition from Map phase --> Reduce phase only when all maps are done
	-Then we can signal job completion via DONE() so main/corrdinator.go can exit

Concurreny:
	- Go's RPC server runs each handler in its own go-routine.
	- We protect shared state(arrays , flags) with a single mutex.

*/

import (
	"net" // Listen on a UNIX-domain socket file.
	"net/http"
	"net/rpc" // Expose methods as RPC endpoints (HTTP over UNIX socket).
	"os"      // Remove old socket before binding.
	"sync"    // Mutex to guard shared state from concurrent RPC handlers.
	"time"    // Timeouts for rescheduling tasks.
)

// taskstate enumerates lifecycle of single scheduled task in a phase
type taskState int

const (
	stateIdle       taskState = iota //not yet assigned to any worker
	stateInProgress                  // handed to a worker; waiting for completion
	stateDone                        // finished successfully (reported Ok=true)
)

// SchedTask is the coordinator's internal record for one task(Map or Reduce)
type schedTask struct {
	id        int       // 0 to nMap-1 for maps , 0 to nReduce-1 for reduces
	files     []string  //for maps the single input file to process reduces dont need files
	state     taskState // it is a lifecycle flag
	startTime time.Time // when assigned used to detect timeouts
}

// coordinator aggregates all job state and implements the RPC handlers
type Coordinator struct {
	mu sync.Mutex // single lock to protect all the shared data

	// immutable job config
	nMap    int      //total number of map tasks
	nReduce int      // total number of reduce tasks
	inputs  []string // list of input files (one per map task)

	// per phase task tables
	maps    []schedTask //length nMap
	reduces []schedTask //length nReduce

	//phase progress flag
	mapPhaseOK bool //becomes true once all map tasks reach stateDone

}

// MakeCoordinator is called by main/mrCoordinator.go with input files and nRecude
// it inistializes task tables, starts the RPC SERVER and return a pointers used
func MakeCordinator(files []string, nReduce int) *Coordinator {
	// we have to allocate and initialize the coordinator struct
	c := &Coordinator{
		nMap:       len(files),
		nReduce:    nReduce,
		inputs:     files,
		maps:       make([]schedTask, len(files)),
		reduces:    make([]schedTask, nReduce),
		mapPhaseOK: false,
	}

	//initialize map tasks one input per task
	for i, f := range files {
		c.maps[i] = schedTask{
			id:    i,
			files: []string{f},
			state: stateIdle,
		}
	}

	// it starts the rpc server in the background sp wokers can connect immediately
	c.server()
	return c
}

// Done is method that polled by main/coordinator.go to decide when to exit
// it must return true only when the entire job is finished

func (c *Coordinator) Done() bool {
	c.mu.Lock()         // guard shared state so we locked it
	defer c.mu.Unlock() // defer will run after done function compeletes

	// If we have not even completed the map phase job isnt done
	if !c.mapPhaseOK {
		return false
	}

	// we have to ensure all reduce tasks are marked done
	for _, t := range c.reduces {
		if t.state != stateDone {
			return false
		}
	}
	// All done --> main/mrcoordinator.go will exit , which also pompts workers to exit
	return true
}

//RequestTask is invoked by workers to fetch work
// its working as follows
// requeue any timed out rasks which timedout like > 10s since assignment
// if map phase is not done: we have to hand out an IDLE map task if any
// --> if no idle maps and not all maps DOne yet --> tell worker to wait
// -->  if all maps done --> flip to reduce phase
// -- if reduce phase : we have to hand out an IDLE reduce task
//    * if not then TASKEXIT if all reduces done
//     * else TaskNone to wait

func (c *Coordinator) RequestTask(_ *RequestTaskArgs, rep *RequestTaskReply) error {
	c.mu.Lock()         // protect shared state from concurrent requests
	defer c.mu.Unlock() // release lock before returning

	c.reapTimeoutsLocked(10 * time.Second) // reschedule task that exceeded the deadline

	// Map phase
	if !c.mapPhaseOK {
		// first we Try to assign an IDLE map task
		if t, ok := c.pickLocked(c.maps); ok {
			// assign the map
			rep.Type = TaskMap
			rep.TaskID = t.id
			rep.Files = t.files     //single input file
			rep.NReduce = c.nReduce //needed by worker to partition outputs
			rep.NMap = c.nMap       // informationl
			return nil              // success
		}

		//IF no idle map available right now
		if c.allDoneLocked(c.maps) {
			// change flag to true i.e. map phase is DONE
			c.mapPhaseOK = true
		} else {
			//maps are still in progress  -->
			rep.Type = TaskNone
			return nil
		}
	}

	//Reduce Phase
	// Try to assign an IDLE reduce task
	if t, ok := c.pickLocked(c.reduces); ok {

		rep.Type = TaskReduce
		rep.TaskID = t.id
		rep.NReduce = c.nReduce // for symmetry ; worker doesnt need files
		rep.NMap = c.nMap
		return nil
	}

	// No Idle reduce right now: if all reduces are done , tell workers to exits or else ask to wait
	if c.allDoneLocked(c.reduces) {
		rep.Type = TaskExit
	} else {
		rep.Type = TaskNone
	}
	return nil
}

//ReportTask is invoked by a worker after finishing a task
// we only mark a task as DONE if it was previously InProgress and the report says OK= True

func (c *Coordinator) ReportTask(args *ReportTaskArgs, _ *ReportTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	//choose which task table we are updating based on type
	var table *[]schedTask
	switch args.Type {
	case TaskMap:
		table = &c.maps
	case TaskReduce:
		table = &c.reduces
	default:
		// if unknown types that keeps handler robust
		return nil
	}

	// Updating the referenced task if its currently inProgress.
	t := &(*table)[args.TaskID]
	if t.state == stateInProgress && args.Ok {
		t.state = stateDone
		// we do not flip mapPhaseOK here; that is handled in RequestTask when all maps are done
	}
	// if ok ==false we simply leave it as inProgress
	return nil
}

//pickLocked scans a task slice for an IDLE TASk, marks it inProgress, timeStamps it and returns it
//If none are IDle returns (zero,false). caller must hold c.mu.

func (c *Coordinator) pickLocked(ts []schedTask) (schedTask, bool) {
	for i := range ts {
		if ts[i].state == stateIdle {
			ts[i].state = stateInProgress // mark as assigned
			ts[i].startTime = time.Now()  // for timeout detection
			return ts[i], true            // hand out a copy
		}
	}
	return schedTask{}, false //nothing idle right now
}

// allDoneLocked returns true only if every task in the slice is stateDone. Caller must hold c.mu.
func (c *Coordinator) allDoneLocked(ts []schedTask) bool {
	for _, t := range ts {
		if t.state != stateDone {
			return false
		}
	}
	return true
}

// This handles workers that crashed, hung, or are too slow to be useful.
func (c *Coordinator) reapTimeoutsLocked(limit time.Duration) {
	now := time.Now()
	// Check Map tasks
	for i := range c.maps {
		if c.maps[i].state == stateInProgress && now.Sub(c.maps[i].startTime) > limit {
			c.maps[i].state = stateIdle // reschedule this task for another worker
		}
	}
	// Check Reduce tasks
	for i := range c.reduces {
		if c.reduces[i].state == stateInProgress && now.Sub(c.reduces[i].startTime) > limit {
			c.reduces[i].state = stateIdle // reschedule
		}
	}
}

// server starts an HTTP=RPC SERVER bound to a unix domain socket path
// removes any stale socket file to avoid eaddrinuse
// serves RPC handlers concurrently

func (c *Coordinator) server() {
	// register all exported methods on coordiantor as rpc endpoints
	_ = rpc.Register(c)

	// Arrange to server RPC over HTTP
	rpc.HandleHTTP()

	//compute socket path and ensure no stale file exits
	sock := coordinatorSock()

	_ = os.Remove(sock)

	//listen on unix-domain socket instead of TCP SORT
	l, e := net.Listen("unix", sock)

	if e != nil {
		// cannot proceed without a listening endpoint
		panic(e)
	}

	//serve HTTP requests in background
	go http.Serve(l, nil)
}
