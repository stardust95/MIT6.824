package mapreduce

import (
	"fmt"
	"sync"
	"log"
)

var wg sync.WaitGroup
//
// schedule() starts and waits for all tasks in the given phase (Map
// or Reduce). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var nOther int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		nOther = nReduce
	case reducePhase:
		ntasks = nReduce
		nOther = len(mapFiles)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, nOther)

	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	//
	for index := 0; index < ntasks; index++ {
		address := <- registerChan
		wg.Add(1)
		taskNum := index
		go func (address string, file string){
			defer wg.Done()		// execute first to make sure wg.Done() will be called even fail

			ok := call(address, "Worker.DoTask", 
				&DoTaskArgs{jobName, file, phase, taskNum, nOther}, nil)
			if ok {
				go func(){
					registerChan <- address
				}()
			}else{
				log.Fatal("calling failed")
			}
		}(address, mapFiles[index])
	}

	wg.Wait()
	
	fmt.Printf("Schedule: %v phase done\n", phase)
}

