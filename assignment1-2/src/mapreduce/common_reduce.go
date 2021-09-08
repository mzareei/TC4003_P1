package mapreduce

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"sort"
	"strings"

	"github.com/olivere/ndjson"
)

// doReduce does the job of a reduce worker: it reads the intermediate
// key/value pairs (produced by the map phase) for this task, sorts the
// intermediate key/value pairs by key, calls the user-defined reduce function
// (reduceF) for each key, and writes the output to disk.
func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTaskNumber int, // which reduce task this is
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	// TODO:
	// You will need to write this function.
	// You can find the intermediate file for this reduce task from map task number
	// m using reduceName(jobName, m, reduceTaskNumber).
	// Remember that you've encoded the values in the intermediate files, so you
	// will need to decode them. If you chose to use JSON, you can read out
	// multiple decoded values by creating a decoder, and then repeatedly calling
	// .Decode() on it until Decode() returns an error.
	//
	// You should write the reduced output in as JSON encoded KeyValue
	// objects to a file named mergeName(jobName, reduceTaskNumber). We require
	// you to use JSON here because that is what the merger than combines the
	// output from all the reduce tasks expects. There is nothing "special" about
	// JSON -- it is just the marshalling format we chose to use. It will look
	// something like this:
	//
	// enc := json.NewEncoder(mergeFile)
	// for key in ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	//
	// Use checkError to handle errors.
	// fmt.Println("doReduce")

	key_value_map := make(map[string][]string)
	reducer_value_keys := []string{}

	key_value_map = mr_shuffle(nMap, jobName, reduceTaskNumber, key_value_map)
	reducer_value_keys = mr_sort(key_value_map, reducer_value_keys)
	mr_merge(jobName, reduceTaskNumber, reducer_value_keys, key_value_map, reduceF)
}

func mr_shuffle(mappers int, jobName string, reduceTaskNumber int, key_value_map map[string][]string) map[string][]string {
	for i := 0; i < mappers; i++ {
		reduce_name := reduceName(jobName, i, reduceTaskNumber)
		content, err := ioutil.ReadFile(reduce_name)
		if err != nil {
			log.Fatal(err)
		}

		texto := string(content)
		r := ndjson.NewReader(strings.NewReader(texto))
		for r.Next() {
			var kv KeyValue
			if err := r.Decode(&kv); err != nil {
				fmt.Fprintf(os.Stderr, "Decode failed: %v", err)
			}

			if _, ok := key_value_map[kv.Key]; ok {
				key_value_map[kv.Key] = append(key_value_map[kv.Key], kv.Value)
			} else {
				key_value_map[kv.Key] = []string{}
			}
		}
		if err := r.Err(); err != nil {
			fmt.Fprintf(os.Stderr, "Reader failed: %v", err)
		}
	}
	return key_value_map
}

func mr_sort(key_value_map map[string][]string, reducer_value_keys []string) []string {
	for key := range key_value_map {
		reducer_value_keys = append(reducer_value_keys, key)
	}
	sort.Strings(reducer_value_keys)
	return reducer_value_keys
}

func mr_merge(jobName string, reduceTaskNumber int, reducer_value_keys []string, key_value_map map[string][]string, reduceF func(key string, values []string) string) {
	merge_name := mergeName(jobName, reduceTaskNumber)
	merge_file, err := os.Create(merge_name)
	if err != nil {
		log.Fatal(err)
	}

	for _, merge_key := range reducer_value_keys {
		merged_values := reduceF(merge_key, key_value_map[merge_key])
		json.NewEncoder(merge_file).Encode(KeyValue{merge_key, merged_values})
	}

	merge_file.Close()
}
