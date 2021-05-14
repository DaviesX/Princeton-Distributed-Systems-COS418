package mapreduce

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"os"
)

// doMap does the job of a map worker: it reads one of the input files
// (inFile), calls the user-defined map function (mapF) for that file's
// contents, and partitions the output into nReduce intermediate files.
func doMap(
	jobName string, // The name of the MapReduce job
	mapTaskNumber int, // Which map task this is
	inFile string, // File name of the input file.
	nReduce int, // The number of reduce task that will be run ("R" in the paper)
	mapF func(file string, contents string) []KeyValue,
) {
	fileContent, err := ioutil.ReadFile(inFile)
	if err != nil {
		fmt.Printf(
			"Failed to read file=%s, err=%s\n",
			inFile,
			err.Error())
		panic(err)
	}

	// Map file content to key-value pairs.
	kvs := mapF(inFile, string(fileContent))

	// Creates per-reducer JSON serializer.
	encoders := make([]*json.Encoder, nReduce)
	for i := 0; i < nReduce; i++ {
		outputFileName := reduceName(jobName, mapTaskNumber, i)

		reducerFile, err := os.OpenFile(
			outputFileName, os.O_CREATE|os.O_WRONLY, 0777)
		if err != nil {
			fmt.Printf(
				"Faild to open file=%s in write mode, err=%s\n",
				outputFileName, err.Error())
			panic(err)
		}

		defer reducerFile.Close()

		encoders[i] = json.NewEncoder(reducerFile)
	}

	// Put each key-value pair to its corresponding reducer file
	// sharded by key.
	for _, kv := range kvs {
		reducerTaskNumber := ihash(kv.Key) % uint32(nReduce)
		err := encoders[reducerTaskNumber].Encode(&kv)

		if err != nil {
			fmt.Printf(
				"Failed to encode (k=%s, v=%s), err=%s\n",
				kv.Key,
				kv.Value,
				err.Error())
			panic(err)
		}
	}
}

func ihash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}
