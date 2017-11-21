package mapreduce

import (
    "fmt"
    "sync"
)

// schedule() starts and waits for all tasks in the given phase (Map
// or Reduce). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.

func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)

	freeMachineChan := make(chan string, 100)

    go func () {
        for {
			machine := <-registerChan
			fmt.Println("Schedule: Registering machine", machine)
			freeMachineChan <- machine
        }
    }()

	var wg sync.WaitGroup

	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
		wg.Add(ntasks)
		t_num := 0
        for t_num < ntasks {
			//fmt.Println("Schedule: In Map Phase", ntasks, n_other, t_num)
            go func(t_n int) {
				for {
					w := <-freeMachineChan
					doTaskArgs := &DoTaskArgs{
								JobName			:	jobName,
								File			:	mapFiles[t_n],
								Phase			:	phase,
								TaskNumber		:	t_n,
								NumOtherPhase	:	n_other}
					//fmt.Println("Schedule: doTaskArgs", doTaskArgs)
					var reply struct{}
					ok := call(w, "Worker.DoTask", doTaskArgs, &reply)
					go func() {
						freeMachineChan <- w
					}()
					if ok {
						wg.Done()
						break
					}
				}
            }(t_num)
            t_num += 1
        }
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
		wg.Add(ntasks)
		t_num := 0
        for t_num < ntasks {
			//fmt.Println("Schedule: In Reduce Phase", ntasks, n_other, t_num)
            go func(t_n int) {
                for {
					w := <-freeMachineChan
                    doTaskArgs := &DoTaskArgs{
                                JobName         :   jobName,
                                Phase           :   phase,
                                TaskNumber      :   t_n,
                                NumOtherPhase   :   n_other}
					//fmt.Println("Schedule: doTaskArgs", doTaskArgs)
					var reply struct{}
                    ok := call(w, "Worker.DoTask", doTaskArgs, &reply)
					go func() {
						freeMachineChan <- w
					}()
                    if ok {
						wg.Done()
                        break
                    }
                }
            }(t_num)
            t_num += 1
        }
	}

	wg.Wait()
	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	//
	// TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO
	//
	fmt.Printf("Schedule: %v phase done\n", phase)
}
