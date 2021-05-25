package mapreduce

import (
	"errors"
	"fmt"
	"sync"
)

// Keeps listening on worker registration events until it is told to stop.
func WorkerRegistrationListener(
	workerRegChannel chan string,
	workersSeen map[string]bool,
	idleWorkerPool *chan string,
) {
	for {
		newWorker := <-workerRegChannel
		if newWorker == "gibberish" {
			break
		}

		if workersSeen[newWorker] {
			continue
		}
		workersSeen[newWorker] = true

		*idleWorkerPool <- newWorker
	}
}

func WorkerSourceListener(
	workerRegChannel chan string,
	existingWorkerList []string,
	workerListMutex *sync.Mutex,
	idleWorkerPool *chan string,
) {
	workersSeen := make(map[string]bool)

	// Pull from the existing worker list and add them to the worker pool. It
	// also keeps track of the set of workers added to the pool to avoid
	// duplication from the overlap coming from the pulling in of workers via
	// the registration channel.
	for i := 0; ; i++ {
		workerListMutex.Lock()
		if i == len(existingWorkerList) {
			workerListMutex.Unlock()
			break
		}
		worker_i := existingWorkerList[i]
		workerListMutex.Unlock()

		if workersSeen[worker_i] {
			panic(errors.New("logical error"))
		}
		workersSeen[worker_i] = true

		*idleWorkerPool <- worker_i
	}

	// Pull in workers from the registration channel to handle future new
	// worker registration.
	WorkerRegistrationListener(workerRegChannel, workersSeen, idleWorkerPool)
}

// Launches a new task on the scheduled worker and returns the worker back to
// the pool once the task is complete. If the task fails, it retrieves another
// worker from the worker pool and discards the worker incurs the failure.
func RunTaskOnWorker(
	scheduledWorker string,
	task DoTaskArgs,
	idleWorkerPool *chan string,
	taskWaitGroup *sync.WaitGroup,
) {
	defer taskWaitGroup.Done()

	for {
		ok := call(scheduledWorker, "Worker.DoTask", task, new(struct{}))
		if ok {
			*idleWorkerPool <- scheduledWorker
			break
		}

		scheduledWorker = <-*idleWorkerPool
	}
}

// schedule starts and waits for all tasks in the given phase (Map or Reduce).
func (mr *Master) schedule(phase jobPhase) {
	var ntasks int
	var nios int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mr.files)
		nios = mr.nReduce
	case reducePhase:
		ntasks = mr.nReduce
		nios = len(mr.files)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, nios)

	// Initializes the worker pool and listens events to add new workers to the
	// pool.
	idleWorkerPool := make(chan string, ntasks)
	defer close(idleWorkerPool)

	go WorkerSourceListener(
		mr.registerChannel, mr.workers, &mr.Mutex, &idleWorkerPool)

	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	fmt.Printf("Sheduling at phase=%s\n", phase)

	var taskWaitGroup sync.WaitGroup
	for i := 0; i < ntasks; i++ {
		scheduledWorker := <-idleWorkerPool

		var task DoTaskArgs
		task.JobName = mr.jobName
		task.Phase = phase
		if phase == mapPhase {
			task.File = mr.files[i]
		}
		task.TaskNumber = i
		task.NumOtherPhase = nios

		taskWaitGroup.Add(1)
		go RunTaskOnWorker(
			scheduledWorker, task, &idleWorkerPool, &taskWaitGroup)
	}

	// Sychronizes with workers to make sure all tasks are finished after this
	// point.
	taskWaitGroup.Wait()

	// Stop the worker registration listener since all tasks have been
	// completed.
	mr.registerChannel <- "gibberish"

	fmt.Printf("Schedule: %v phase done\n", phase)
}
