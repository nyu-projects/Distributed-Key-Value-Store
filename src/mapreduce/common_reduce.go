package mapreduce

import (
    //"fmt"
    "sort"
    "os"
    "encoding/json"
//    "io"
    "log"
)

type ByKey []string

func (a ByKey) Len() int            { return len(a) }
func (a ByKey) Swap(i, j int)       { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool  { return a[i] < a[j] }


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

    kvMap := make(map[string][]string)
    var keyList []string
    m := 0
    for m < nMap {
        mapFile := reduceName(jobName, m, reduceTaskNumber)
        f, err := os.Open(mapFile)

        if err != nil {
            log.Fatal("doReduce: Open File ", err)
        }

        dec := json.NewDecoder(f)
        for {
            var kv KeyValue
            err = dec.Decode(&kv)
            if err != nil {
                break
            }

            //fmt.Println("kv: ", kv)
            //kv.Key kv.Value

            _, ok := kvMap[kv.Key]

            if ok {
                kvMap[kv.Key] = append(kvMap[kv.Key], kv.Value)
            } else {
                var kvVal []string
                kvVal = append(kvVal, kv.Value)
                kvMap[kv.Key] = kvVal

                keyList = append(keyList, kv.Key)
            }
        }
        m += 1
        f.Close()
    }

    sort.Sort(ByKey(keyList))

    f, err := os.Create(outFile)
    if err != nil {
		log.Fatal("doReduce: Create outFile", err)
        //fmt.Println("Fatal Error", err)
    } else {
        enc := json.NewEncoder(f)
        for _, k := range keyList {
            rVal := reduceF(k, kvMap[k])
            newKv := KeyValue{k, rVal}
            err := enc.Encode(&newKv)
			if err != nil {
				log.Fatal("doReduce: Encode", err)
			}
        }
        f.Close()
    }

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
}
