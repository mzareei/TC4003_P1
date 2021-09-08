package main

import (
	"fmt"
	"mapreduce"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

// The mapping function is called once for each piece of the input.
// In this framework, the key is the name of the file that is being processed,
// and the value is the file's contents. The return value should be a slice of
// key/value pairs, each represented by a mapreduce.KeyValue.
func mapF(document string, value string) (res []mapreduce.KeyValue) {
	// TODO: you should complete this to do the inverted index challenge
	var IsLetter = regexp.MustCompile(`^[a-zA-Z]+$`).MatchString
	words := strings.FieldsFunc(value, func(word rune) bool {
		return !IsLetter(string(word))
	})
	for _, w := range words {
		kv := mapreduce.KeyValue{Key: w, Value: document}
		res = append(res, kv)
	}
	return res
}

// The reduce function is called once for each key generated by Map, with a
// list of that key's string value (merged across all inputs). The return value
// should be a single output value for that key.
func reduceF(key string, values []string) string {
	// TODO: you should complete this to do the inverted index challenge
	documents := []string{}
	sort.Strings(values)
	duplicated := false
	var last_item string

	for _, v := range values {
		if !duplicated && last_item != v {
			documents = append(documents, v)
			duplicated = false // Como lo ordene anteriormente
			last_item = v
		}
	}

	total_documents := strconv.Itoa(int(len(documents)))

	response := fmt.Sprintf("%s %s", total_documents, strings.Join(documents, ","))
	return response
}

// Can be run in 3 ways:
// 1) Sequential (e.g., go run wc.go master sequential x1.txt .. xN.txt)
// 2) Master (e.g., go run wc.go master localhost:7777 x1.txt .. xN.txt)
// 3) Worker (e.g., go run wc.go worker localhost:7777 localhost:7778 &)
func main() {
	if len(os.Args) < 4 {
		fmt.Printf("%s: see usage comments in file\n", os.Args[0])
	} else if os.Args[1] == "master" {
		var mr *mapreduce.Master
		if os.Args[2] == "sequential" {
			mr = mapreduce.Sequential("iiseq", os.Args[3:], 3, mapF, reduceF)
		} else {
			mr = mapreduce.Distributed("iiseq", os.Args[3:], 3, os.Args[2])
		}
		mr.Wait()
	} else {
		mapreduce.RunWorker(os.Args[2], os.Args[3], mapF, reduceF, 100)
	}
}
