package mapreduce

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sort"
)

func LoadKvs(file *os.File, kvs *[]KeyValue) {
	decoder := json.NewDecoder(file)

	var kv KeyValue
	decoder_err := decoder.Decode(&kv)
	for decoder_err == nil {
		*kvs = append(*kvs, kv)
		decoder_err = decoder.Decode(&kv)
	}
}

func NextKey(
	kvs []KeyValue, i *int) (key string, values []string, err error) {
	if *i >= len(kvs) {
		return "", make([]string, 0), errors.New("end of slice")
	}

	theKey := kvs[*i].Key
	valuesGroupByKey := make([]string, 0)

	for ; *i < len(kvs) && kvs[*i].Key == theKey; *i++ {
		valuesGroupByKey = append(valuesGroupByKey, kvs[*i].Value)
	}

	return theKey, valuesGroupByKey, nil
}

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
	// Collects all the imtermediate data produced from all
	// the mapper tasks.
	kvs := make([]KeyValue, 0)
	for i := 0; i < nMap; i++ {
		inputFileName := reduceName(jobName, i, reduceTaskNumber)
		inputFile, err := os.Open(inputFileName)
		if err != nil {
			fmt.Printf(
				"Failed to open reducer file=%s, err=%s",
				inputFileName,
				err.Error())
			panic(err)
		}

		LoadKvs(inputFile, &kvs)
		inputFile.Close()
	}

	sort.Slice(
		kvs,
		func(i, j int) bool {
			return kvs[i].Key < kvs[j].Key
		})

	// Prepares the output to store the result split for the
	// current reducer task.
	outputFileName := mergeName(jobName, reduceTaskNumber)
	outputFile, err := os.OpenFile(
		outputFileName, os.O_CREATE|os.O_WRONLY, 0777)
	if err != nil {
		fmt.Printf(
			"Failed to open split file=%s, err=%s",
			outputFileName,
			err.Error())
		panic(err)
	}
	defer outputFile.Close()

	outputEncoder := json.NewEncoder(outputFile)

	// Reduce the key-value pairs key by key and commit the
	// reduction to the result split.
	var iterator int = 0
	key, values, err := NextKey(kvs, &iterator)
	for err == nil {
		reductionResult := reduceF(key, values)
		outputEncoder.Encode(KeyValue{key, reductionResult})

		key, values, err = NextKey(kvs, &iterator)
	}
}
