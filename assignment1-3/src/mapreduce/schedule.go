package mapreduce

import (
	"context"
)

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
	debug("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, nios)

	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	//
	// TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO

	end := make(chan bool, ntasks)

	for i := 0; i < ntasks; i++ {
		go func(number int) {

			var worker string
			ok := false
			for ok != true {
				worker = <-mr.registerChannel
				ok = call(worker, "Worker.DoTask", DoTaskArgs{mr.jobName, mr.files[number], phase, number, nios}, new(struct{}))
			}
			end <- true
			mr.registerChannel <- worker
		}(i)
	}

	for i := 0; i < ntasks; i++ {
		<-end
	}

	close(end)

	// ctx, cancel := context.WithCancel(context.Background())
	// done := make(chan bool)
	// defer cancel()

	// for n := range gen(ctx) {
	// 	if n == ntasks {
	// 		break
	// 	} else {
	// 		go func(number int) {
	// 			var worker string
	// 			argumentos := DoTaskArgs{mr.jobName, mr.files[n-1], phase, n, nios}
	// 			ok := false
	// 			for ok != true {
	// 				worker = get_next_worker(mr)
	// 				ok = call(worker, "Worker.DoTask", argumentos, new(struct{}))
	// 			}
	// 			done <- true
	// 		}(n)
	// 	}
	// }

	// close(done)

	// for i := 0; i < ntasks; i++ {
	// 	<-done
	// }

	debug("Schedule: %v phase done\n", phase)
}

func gen(ctx context.Context) <-chan int {
	dst := make(chan int)
	n := 0
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case dst <- n:
				n++
			}
		}
	}()
	return dst
}
