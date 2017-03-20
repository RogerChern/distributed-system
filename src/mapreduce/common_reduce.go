package mapreduce

import (
	"encoding/json"
	"os"
	"sort"
	"log"
)

// doReduce manages one reduce task: it reads the intermediate
// key/value pairs (produced by the map phase) for this task, sorts the
// intermediate key/value pairs by key, calls the user-defined reduce function
// (reduceF) for each key, and writes the output to disk.
func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTaskNumber int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	//
	// You will need to write this function.
	//
	// You'll need to read one intermediate file from each map task;
	// reduceName(jobName, m, reduceTaskNumber) yields the file
	// name from map task m.
	//
	// Your doMap() encoded the key/value pairs in the intermediate
	// files, so you will need to decode them. If you used JSON, you can
	// read and decode by creating a decoder and repeatedly calling
	// .Decode(&kv) on it until it returns an error.
	//
	// You may find the first example in the golang sort package
	// documentation useful.
	//
	// reduceF() is the application's reduce function. You should
	// call it once per distinct key, with a slice of all the values
	// for that key. reduceF() returns the reduced value for that key.
	//
	// You should write the reduce output as JSON encoded KeyValue
	// objects to the file named outFile. We require you to use JSON
	// because that is what the merger than combines the output
	// from all the reduce tasks expects. There is nothing special about
	// JSON -- it is just the marshalling format we chose to use. Your
	// output code will look something like this:
	//
	// enc := json.NewEncoder(file)
	// for key := ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	//
	outFileHandler, err := os.Create(outFile)
	if err != nil {
		log.Fatal("os.Create: can not create ", err)
	}
	enc := json.NewEncoder(outFileHandler)
	defer outFileHandler.Close()
	var kvPairs []KeyValue
	for i := 0; i < nMap; i++ {
		inFileName := reduceName(jobName, i, reduceTaskNumber)
		inFileHandler, err := os.OpenFile(inFileName, os.O_RDONLY, 0400)
		if err != nil {
			log.Fatal("os.OpenFile: ", err)
		}
		dec := json.NewDecoder(inFileHandler)
		kv := KeyValue{}
		for {
			err := dec.Decode(&kv)
			if err != nil {
				break
			} else {
				kvPairs = append(kvPairs, kv)
			}
		}
		inFileHandler.Close()
	}
	sort.Sort(ByKey(kvPairs))
	stop := 0
	for stop != len(kvPairs) {
		start := stop
		stop = start + 1
		var values []string
		for ; stop != len(kvPairs) && kvPairs[stop].Key == kvPairs[stop - 1].Key; stop++ {
			values = append(values, kvPairs[stop - 1].Value)
		}
		values = append(values, kvPairs[stop - 1].Value)
		res := reduceF(kvPairs[start].Key, values)
		enc.Encode(KeyValue{kvPairs[start].Key, res})
	}
}

type ByKey []KeyValue
func (b ByKey) Len() int { return len(b) }
func (b ByKey) Swap(i, j int) { b[i], b[j] = b[j], b[i] }
func (b ByKey) Less(i, j int) bool { return b[i].Key < b[j].Key }