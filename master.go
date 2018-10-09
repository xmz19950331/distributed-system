package mapreduce
import "container/list"
import "fmt"

type WorkerInfo struct {
  address string
  // You can add definitions here.
}


// Clean up all workers by sending a Shutdown RPC to each one of them Collect
// the number of jobs each work has performed.
func (mr *MapReduce) KillWorkers() *list.List {
  l := list.New()
  for _, w := range mr.Workers {
    DPrintf("DoWork: shutdown %s\n", w.address)
    args := &ShutdownArgs{}
    var reply ShutdownReply;
    ok := call(w.address, "Worker.Shutdown", args, &reply)
    if ok == false {
      fmt.Printf("DoWork: RPC %s shutdown error\n", w.address)
    } else {
      l.PushBack(reply.Njobs)
    }
  }
  return l
}

func (mr *MapReduce) callWorkerDoJob(jobid int, jobType string){
  worker := <- mr.registerChannel
  var Args DoJobArgs
  if jobType == "map"{
    //Args :=
    Args = DoJobArgs{File: mr.file, Operation: Map, JobNumber: jobid, NumOtherPhase: mr.nReduce}
    //fmt.Println(Args)
  }else{
    Args = DoJobArgs{File: mr.file, Operation: Reduce, JobNumber: jobid, NumOtherPhase: mr.nMap}
    //fmt.Println(Args)
  }
  jobReply := new(DoJobReply)
  ok := call(worker, "Worker.DoJob", Args, &jobReply)
  if (ok == false || jobReply.OK == false) {
    go mr.callWorkerDoJob(jobid, jobType)
  }else{
    if jobType == "map"{
      mr.mapChannel <- true
    }else{
      mr.reduceChannel <- true
    }
    mr.registerChannel <- worker //put worker back
  }
  // fmt.Println(jobid)
  // fmt.Println(" done")
  
}



func (mr *MapReduce) RunMaster() *list.List {
  // Your code here
  //do map
  //fmt.Println(mr.nMap)
  for i := 0; i < mr.nMap; i++ {
    // go func(i int) {
    //     worker := <- mr.registerChannel
    //     mapArgs := DoJobArgs{File: mr.file, Operation: Map, JobNumber: i, NumOtherPhase: mr.nReduce}
    //     jobReply := new(DoJobReply)
    //     ok := call(worker, "Worker.DoJob", mapArgs, &jobReply)
    //     mr.mapChannel <- true
    //     mr.registerChannel <- worker //put that back
    // }(i)
    go mr.callWorkerDoJob(i, "map")
  }
  //wait until map complete
  for i := 0; i< mr.nMap; i++{
    <- mr.mapChannel
  }
  //fmt.Println("Map done")

  //do reduce
  for i := 0; i < mr.nReduce; i++ {
    //reduceArgs := DoJobArgs{File: mr.file, Operation: Reduce, JobNumber: i, NumOtherPhase: mr.nMap}
    //mr.nReduce = i
    //go func(i int){
    //     worker := <- mr.registerChannel
    //     reduceArgs := DoJobArgs{File: mr.file, Operation: Reduce, JobNumber: i, NumOtherPhase: mr.nMap}
    //     jobReply := new(DoJobReply)
    //     ok := call(worker, "Worker.DoJob", reduceArgs, &jobReply)
    //     mr.reduceChannel <- true
    //     mr.registerChannel <- worker
    // }(i)
    go mr.callWorkerDoJob(i, "reduce")
  }
  //wait until reduce complete
  for i := 0; i< mr.nReduce; i++{
    <- mr.reduceChannel
  }
  //fmt.Println("Reduce done")

  return mr.KillWorkers()
}
