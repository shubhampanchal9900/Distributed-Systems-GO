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

		case TaskExit:
			// coordinator declared job finished : terminate worker loop
              return
		}
	}
}

// report sends a completion or failure notification to the coordinatr
// we ignore the RPC success here iff coordinator is gone, jobs over anyway

func report(tt TaskType, id int , ok bool){
	args := ReportTaskArgs(Type: tt, TaskID: id, OK:ok) //prepare payload
	rep :=ReportTaskReply{}                             // empty reply struct
	_ = call("Coordinator.RepotTask", &args,&rep)        //  notification
}

//doMap runs a single map task
// it reads the input fully
// call mapf to produce []KeyValue
// partitions by bucket = ihash(key) % nReduce.
// we write each bucket to its own json file mr-<mapID>-<reduceID> using temp+rename.

func doMap(mapID int ,filename string, nReduce int, mapf func(string,string) []KeyValue) error{

	//Read the entire input file into memory
	data, err :=os.ReadFile(filename)
	if err !=nil{
		return err // Coordinator will reschedule on timeout or failure .we return error
	}

	// we have to run user supplied map function from plugin((((oh user gave me logic))))
	kva := mapf(filename, string(data))

	// prepare nReduce buckets: buckets[r] collects all kV whose hash maps to r.
	buckets := make([][]KeyValue,nReduce)

	for _, kv := range kva{
		r:= ihash(kv.key) % nReduce // reduce partition for this key
		buckets[r] =append(buckets[r],kv)
	}

	//for each reduce bucket r , write a jsob stream of keyvalue obj
	// we will use temp file to avoid partial completion
	for r :=0; r< nReduce; r++{
		//create a temp file in default temp dir
		tmp, err := os.CreateTemp("", fmt.Sprintf("mr-%d-%d-*", mapID, r))
		if err != nil {
			return err
		}

		//Stream-encode each KV in this bucket to JSON FILES
		enc := json.NewEncoder(tmp)
		for _, kv := range buckets[r]{
	// On any encode error, close and propagate error so coordinator can reschedule.
			if err := enc.Encode(&kv) ; err !=nil{
				tmp.Close()
				return err
			}
		}

		//ensure flush to disk and close file handle 
		if err := tmp.Close(); err != nil{
			return err
		}

		//Compute final filename mr-<mapID>-<reduceID>
		final := fmt.Sprintf("mr-%d-%d",mapID,r)

		//Atomically we have to rename temp file to final file 
		//This avoids other processes reading partially written output
		if err := os.rename(tmp.name(),final); err !=nil{
			return err
		}

	}
	return nil


}

//doReduce runs a single Task:
// collects all intermediate files matching mr-*-%d for this reduceID
// Decides JSON strams into map[key][] values
// Sorts keys deterministically
// ans then calls reducef for each key and writes mr-out-<reduceID> 
func doReduce(reduceID int,reducef func(string,[]string) string) error{
	//first discover all partitions for this reduce bucket produced by all maps
	// local shared filesystem
	files, _ := filepath.Glob(fmt.Sprintf("mr-*-%d",reduceID))

	//kmap collects all values grouped by key across al contributing map tasks
	kmap := make(map[string][]string)
	for _, f: range files{
		fd, err := os.Open(f)
		if err !=nil{
			return err
		}
		
	}

}
