package mr

import (
	"encoding/json" // Efficient streaming JSON encoding/decoding of KeyValue.
	"fmt"           // For formatted I/O (printf for output lines).
	"hash/fnv"      // Non-cryptographic hash to bucket keys into reduce partitions.
	"io"            // Detect io.EOF while decoding JSON streams.
	"net/rpc"       // Go's standard library RPC client (over HTTP on UNIX socket).
	"os"            // File operations (read, write, create temp, rename).
	"path/filepath" // Glob mr-*-%d patterns to collect intermediate files for a reduce bucket.
	"sort"          // Sort keys deterministically before writing reduce outputs.
	"time"          // Sleep between polls (when TaskNone) to avoid hot loop.
)

// ihash determines which reduce bucket a given key belongs to
// it must be consistent across all workers
// it must distribute keys across nreduce paritions
func ihash(key string) int {
	h := fnv.New32a()                  // it creates a new 32 but FNV-1A HASHER more on FNV-1A on bighead.in
	_, _ = h.Write([]byte(key))        // write key  bytes  ignore error (write on hash never fails )
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
			time.Sleep(200 * time.Millisecond)

		case TaskExit:
			// coordinator declared job finished : terminate worker loop
			return
		}
	}
}

// report sends a completion or failure notification to the coordinatr
// we ignore the RPC success here iff coordinator is gone, jobs over anyway

func report(tt TaskType, id int, ok bool) {
	args := ReportTaskArgs{Type: tt, TaskID: id, OK: ok} //prepare payload
	rep := ReportTaskReply{}                             // empty reply struct
	_ = call("Coordinator.RepotTask", &args, &rep)       //  notification
}

//doMap runs a single map task
// it reads the input fully
// call mapf to produce []KeyValue
// partitions by bucket = ihash(key) % nReduce.
// we write each bucket to its own json file mr-<mapID>-<reduceID> using temp+rename.

func doMap(mapID int, filename string, nReduce int, mapf func(string, string) []KeyValue) error {

	//Read the entire input file into memory
	data, err := os.ReadFile(filename)
	if err != nil {
		return err // Coordinator will reschedule on timeout or failure .we return error
	}

	// we have to run user supplied map function from plugin((((oh user gave me logic))))
	kva := mapf(filename, string(data))

	// prepare nReduce buckets: buckets[r] collects all kV whose hash maps to r.
	buckets := make([][]KeyValue, nReduce)

	for _, kv := range kva {
		r := ihash(kv.key) % nReduce // reduce partition for this key
		buckets[r] = append(buckets[r], kv)
	}

	//for each reduce bucket r , write a jsob stream of keyvalue obj
	// we will use temp file to avoid partial completion
	for r := 0; r < nReduce; r++ {
		//create a temp file in default temp dir
		tmp, err := os.CreateTemp("", fmt.Sprintf("mr-%d-%d-*", mapID, r))
		if err != nil {
			return err
		}

		//Stream-encode each KV in this bucket to JSON FILES
		enc := json.NewEncoder(tmp)
		for _, kv := range buckets[r] {
			// On any encode error, close and propagate error so coordinator can reschedule.
			if err := enc.Encode(&kv); err != nil {
				tmp.Close()
				return err
			}
		}

		//ensure flush to disk and close file handle
		if err := tmp.Close(); err != nil {
			return err
		}

		//Compute final filename mr-<mapID>-<reduceID>
		final := fmt.Sprintf("mr-%d-%d", mapID, r)

		//Atomically we have to rename temp file to final file
		//This avoids other processes reading partially written output
		if err := os.Rename(tmp.Name(), final); err != nil {
			return err
		}

	}
	return nil
}

// doReduce runs a single Task:
// collects all intermediate files matching mr-*-%d for this reduceID
// Decides JSON strams into map[key][] values
// Sorts keys deterministically
// ans then calls reducef for each key and writes mr-out-<reduceID>
func doReduce(reduceID int, reducef func(string, []string) string) error {
	//first discover all partitions for this reduce bucket produced by all maps
	// local shared filesystem
	files, _ := filepath.Glob(fmt.Sprintf("mr-*-%d", reduceID))

	//kmap collects all values grouped by key across al contributing map tasks
	kmap := make(map[string][]string)
	for _, f := range files {
		fd, err := os.Open(f)
		if err != nil {
			return err
		}

		//we have to streaming decoder over the file
		dec := json.NewDecoder(fd)
		for {
			var kv KeyValue
			// Decode one keyvalue at a time
			if err := dec.Decode(&kv); err != nil {
				if err == io.EOF {
					break
				}
				fd.Close()
				return err
			}
			//append value to the list for this key
			kmap[kv.Key] = append(kmap[kv.Key], kv.Value)
		}

		//close current partition file
		if err := fd.Close(); err != nil {
			return err
		}
	}

	// Extract and sort unique keys so output lines are deterministic.
	keys := make([]string, 0, len(kmap))
	for k := range kmap {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	// we will Create output with temp+ rename to avoid partial reads
	tmp, err := os.CreateTemp("", fmt.Sprintf("mr-out-%d-*", reduceID))
	if err != nil {
		return err
	}

	for _, k := range keys {
		//user defined combine  over []string
		out := reducef(k, kmap[k])

		if _, err := fmt.Fprintf(tmp, "%v %v\n", k, out); err != nil {
			tmp.Close()
			return err
		}

	}

	// we will close the temp file to ensure contents are flushed
	if err := tmp.Close(); err != nil {
		return err
	}

	//atomically publish final output name
	final := fmt.Sprintf("mr-out-%d", reduceID)
	return os.Rename(tmp.Name(), final)
}

//call dials the coordinator rpc server over a unix domain HTTP connection
// sends method rpcname with args and fills reply
// return false if we could not reach the server or if the call errored

func call(rpcname string, args interface{}, reply interface{}) bool {
	// build the same socket path that coordinator is listerning on
	sock := coordinatorSock()

	// later we can dial an HTTP-RPC connection over unix domain socket which are faster than tcp avoids ports
	c, err := rpc.DialHTTP("unix", sock)
	if err != nil {
		//when coordinator exits ,this will fail worker should exit
		return false
	}

	defer c.Close()

	//we are invoking remote method with arguments fill reply on success.
	if err := c.Call(rpcname, args, reply); err != nil {
		//if the call fails then we treat as not ok so outer logic exits
		return false
	}
	return true
}
