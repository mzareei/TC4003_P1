package mapreduce

import (
	"encoding/json"
	"hash/fnv"
	"io/ioutil"
	"log"
	"os"
)

// go test -run Sequential
// doMap does the job of a map worker: it reads one of the input files
// (inFile), calls the user-defined map function (mapF) for that file's
// contents, and partitions the output into nReduce intermediate files.
func doMap(
	jobName string, // the name of the MapReduce job
	mapTaskNumber int, // which map task this is
	inFile string,
	nReduce int, // the number of reduce task that will be run ("R" in the paper)
	mapF func(file string, contents string) []KeyValue,
) {
	// TODO:
	// You will need to write this function.
	// You can find the filename for this map task's input to reduce task number
	// r using reduceName(jobName, mapTaskNumber, r). The ihash function (given
	// below doMap) should be used to decide which file a given key belongs into.
	//
	// The intermediate output of a map task is stored in the file
	// system as multiple files whose name indicates which map task produced
	// them, as well as which reduce task they are for. Coming up with a
	// scheme for how to store the key/value pairs on disk can be tricky,
	// especially when taking into account that both keys and values could
	// contain newlines, quotes, and any other character you can think of.
	//
	// One format often used for serializing data to a byte stream that the
	// other end can correctly reconstruct is JSON. You are not required to
	// use JSON, but as the output of the reduce tasks *must* be JSON,
	// familiarizing yourself with it here may prove useful. You can write
	// out a data structure as a JSON string to a file using the commented
	// code below. The corresponding decoding functions can be found in
	// common_reduce.go.
	//
	//   enc := json.NewEncoder(file)
	//   for _, kv := ... {
	//     err := enc.Encode(&kv)
	//
	// Remember to close the file after you have written all the values!
	// Use checkError to handle errors.
	// fmt.Println("doMap")

	var file_pointer []*os.File = make([]*os.File, nReduce)

	content, err := ioutil.ReadFile(inFile)
	if err != nil {
		log.Fatal(err)
	}

	texto := string(content)
	new_content := mapF(inFile, texto)

	// Se entra al documento, para el caso de TestSequentialSingle solamente se manda
	// un archivo, para el caso de TestSequentialMany se mandan 5 archivos que se trabaja
	// con 3 reducers cada uno

	// Los archivos se reciben de la siguiente forma para las tareas:
	// - 	TestSequentialSingle se tiene un input llamado "824-mrinput-0.txt" el cual tendra valores del 1 al 100,000
	// 		y da un output que en un inicio estara vacio hasta llegar al range llamado "mrtmp.test-0-0" que tendra el
	//		contenido de key-value del valor 1 al 100,000
	// -	TestSequentialMany se tiene 5 (dividido de 20k en 20k hasta 100k) inputs llamados ["824-mrinput-0.txt",
	// 		"824-mrinput-1.txt","824-mrinput-2.txt","824-mrinput-3.txt","824-mrinput-4.txt"] y da 15 outputs que en un
	// 		inicio estaran vacios hasta llegar al range de new content ["mrtmp.test-0-0","mrtmp.test-0-1","mrtmp.test-0-2",
	// 		"mrtmp.test-1-0","mrtmp.test-1-1","mrtmp.test-1-2","mrtmp.test-2-0","mrtmp.test-2-1","mrtmp.test-2-2",
	// 		"mrtmp.test-3-0","mrtmp.test-3-1","mrtmp.test-3-2","mrtmp.test-4-0","mrtmp.test-4-1","mrtmp.test-4-2",]

	for i := 0; i < nReduce; i++ {
		reduce_name := reduceName(jobName, mapTaskNumber, i)
		create_empty_files(reduce_name, i, file_pointer)
		defer file_pointer[i].Close()
	}

	for _, key_value := range new_content {

		index := ihash(key_value.Key, nReduce)
		err := json.NewEncoder(file_pointer[index]).Encode(&key_value) // NewEconder retorna un puntero a Encoder y recibe un valor del tipo io.Writer
		if err != nil {
			log.Fatal(err)
		}
	}
}

func ihash(s string, r int) int {
	h := fnv.New32a()
	h.Write([]byte(s))
	return int(h.Sum32()) % r
}

func create_empty_files(reduce_name string, index int, file_pointer []*os.File) {
	var err error
	file_pointer[index], err = os.Create(reduce_name)
	if err != nil {
		log.Fatal(err)
	}
}
