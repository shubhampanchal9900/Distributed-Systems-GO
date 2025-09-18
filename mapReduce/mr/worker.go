package mr

import (
	"hash/fnv"
	"time"
)

// ihash determines which reduce bucket a given key belongs to
// it must be consistent across all workers
// it must distribute keys across nreduce paritions
func ihash(key string) int {
	h := fnv.New32a()                  // it creates a new 32 but FNV-1A HASHER more on FNV-1A on bighead.in
	_, _ = h.write([]byte(key))        // write key  bytes  ignore error (write on hash never fails )
	return int(h.Sum32() & 0x7fffffff) // Mask with 0x7fffffff to ensure non-negative int when we cast to signed int
}

//Worker is the main entrypoint called from main/mrworker.go
//The functions mapF and reducef are dynamically loaded from plugin

func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {

	for {
		// step 1. we have to ask coordinator a TASK
		req := RequestTaskArgs{}  // empty request (for future metadata)
		rep := RequestTaskReply{} // This will be filled by RPC
		if ok := call("Cordinator.RequestTask", &req, &rep); !ok {
			// if calling fails the cordinator likely exited

			return
		}

		// step 2 .Act based on the task type provided by co-ordinator
		switch rep.Type {
		case TaskMap:
			// execute the map task over exactly one input file  (rep.Files[0]).
			if err := doMap(rep.TaskID, rep.Files[0], rep.NReduce, mapf); err != nil {
				// if something failed io or encode ---> i will inform the cordinator
				report(TaskMap, rep.TaskID, false)
			} else {
				//successful completion
				report(TaskMap, rep.TaskID, true)
			}
		case TaskReduce:
			// execute the reduce task for this reduceID
			if err := doReduce(rep.TaskID, reducef); err != nil {
				report(TaskReduce, rep.TaskID, false)
			} else {
				report(TaskReduce, rep.TaskID, true)
			}
		case TaskNone:
			//no work now :: avoid busy spinnning and re-ask later
			time.sleep(200 * time.Millisecond)

		}
	}
}
