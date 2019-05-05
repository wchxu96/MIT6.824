package mapreduce

//
// Please do not modify this file.
//

import (
	"fmt"
	"net"
	"sync"
)

// Master holds all the state that the master needs to keep track of.
type Master struct {
	sync.Mutex

	address     string
	doneChannel chan bool

	// protected by the mutex
	newCond *sync.Cond // signals when Register() adds to workers[]
	workers []string   // each worker's UNIX-domain socket name -- its RPC address

	// Per-task information
	jobName string   // Name of currently executing job
	files   []string // Input files
	nReduce int      // Number of reduce partitions

	shutdown chan struct{}
	l        net.Listener
	stats    []int
}

// Register is an RPC method that is called by workers after they have started
// up to report that they are ready to receive tasks.

// RPC 通过 worker 调用注册进workers slice.
// 增加 worker， 为防止race condition
// 全程加锁
func (mr *Master) Register(args *RegisterArgs, _ *struct{}) error {
	mr.Lock()
	defer mr.Unlock()
	debug("Register: worker %s\n", args.Worker)
	mr.workers = append(mr.workers, args.Worker)

	// tell forwardRegistrations() that there's a new workers[] entry.
	mr.newCond.Broadcast()

	return nil
}

// newMaster initializes a new Map/Reduce Master
func newMaster(master string) (mr *Master) {
	mr = new(Master)
	mr.address = master
	mr.shutdown = make(chan struct{})
	mr.newCond = sync.NewCond(mr)
	mr.doneChannel = make(chan bool)
	return
}

// Sequential runs map and reduce tasks sequentially, waiting for each task to
// complete before running the next.

// 注意 distributed 模式 和 sequential 模式的区别
// 这两个函数都调用 run 函数来完成 mapreduce 任务
// 而 run 这个函数就是要等待 mapper 完成相应 任务后
// 才能进行 reduce 任务 ，这个过程是串行进行的
// 这里的顺序执行 和 并行执行的区别是
// sequential 这个函数 不执行 schedule 方法
// 也就是 sequential 是单机版的 MapReduce
// 没什么卵用，是来检验 doMap() 和 doReduce()
// 两个函数的正确情况的。

func Sequential(jobName string, files []string, nreduce int,
	mapF func(string, string) []KeyValue,
	reduceF func(string, []string) string,
) (mr *Master) {
	mr = newMaster("master")
	go mr.run(jobName, files, nreduce, func(phase jobPhase) {
		switch phase {
		case mapPhase:
			for i, f := range mr.files {
				doMap(mr.jobName, i, f, mr.nReduce, mapF)
			} // 将第i个输入文件分片给第i个mapper 执行
		case reducePhase:
			for i := 0; i < mr.nReduce; i++ {
				doReduce(mr.jobName, i, mergeName(mr.jobName, i), len(mr.files), reduceF)
			}
		}
	}, func() {
		mr.stats = []int{len(files) + nreduce}
	})
	return
}

// helper function that sends information about all existing
// and newly registered workers to channel ch. schedule()
// reads ch to learn about workers.
// 使用condition_variable 等待worker 调用rpc方法注册到master中
// 一旦有worker 注册 会mr.newCond.Broadcast() 唤醒所有因等待
// 该事件的go 程， 并通过channel 发送出来
// 这样 将map 任务 schedule 到对应注册的worker 中去

func (mr *Master) forwardRegistrations(ch chan string) {
	i := 0
	for {
		mr.Lock()
		if len(mr.workers) > i {
			// there's a worker that we haven't told schedule() about.
			w := mr.workers[i]
			go func() { ch <- w }() // send without holding the lock.
			i = i + 1
		} else {
			// wait for Register() to add an entry to workers[]
			// in response to an RPC from a new worker.
			mr.newCond.Wait()
		}
		mr.Unlock()
	}
}




// Distributed schedules map and reduce tasks on workers that register with the
// master over RPC.
// 这个函数 是master.go 中另一个最为重要的函数
// 使用一个 channel 来等待 worker 通过 rpc 来注册到 master中
// 这里所谓的 "distributed" 实际指的是这个实现
// 是真的等待 worker 加入并分配任务的真分布式
// 但我的问题是 可能随时有 worker 加入， 那么怎么分割输入文件呢
// 毕竟我们连总共几个worker 都不知道。
// 要注意的是除非是main 函数
// 否则 函数 退出(exit) 后 其中新开的 Goroutine 会继续运行
func Distributed(jobName string, files []string, nreduce int, master string) (mr *Master) {
	mr = newMaster(master)
	mr.startRPCServer()
	go mr.run(jobName, files, nreduce,
		func(phase jobPhase) {
			ch := make(chan string)
			go mr.forwardRegistrations(ch)
			schedule(mr.jobName, mr.files, mr.nReduce, phase, ch)
		},
		func() {
			mr.stats = mr.killWorkers()
			mr.stopRPCServer()
		})
	return
}

// run executes a mapreduce job on the given number of mappers and reducers.
//
// First, it divides up the input file among the given number of mappers, and
// schedules each task on workers as they become available. Each map task bins
// its output in a number of bins equal to the given number of reduce tasks.
// Once all the mappers have finished, workers are assigned reduce tasks.
//
// When all tasks have been completed, the reducer outputs are merged,
// statistics are collected, and the master is shut down.
//
// Note that this implementation assumes a shared file system.
// 这个是master.go 中最为重要的一个函数
// 1. 根据 mapper 的数量 切分 输入文件， 并分配到相应的worker 中
// 2. 每个 worker 需要 根据 reducer 的数量 将它的输出分成多个 bin
// 3. 每个 bin 发送给一个 reducer 进行reduce处理
// 4. merge 合并输出文件
// 例如计算一个大文件中单词出现的次数这种情况
// 1. 切分文件为 len(mapper) 个小块 并把每个文件发送给对应的worker
// 2. worker 开始计算，并将输出结果 分成 len(reducer) 个部分
// 比如 第一个mapper 的输出为 (a:1, aa:10, aaa:100,bb:20)
// merge 中间结果怎么进行？使用hash 函数直接搞定了貌似
// (map function 应该输出的是 [(a,1),(aa,1),(a,1),(a,1)
// (aa,1)] 这种 combine 是从中把所有键相等的键值对进行组合
// 而实际上不组合也是可以的，只是会增加节点间通信负担
// 这里貌似 mapper 直接进行了combine 的操作
// 且 len（reducer） = 2， 那么使用哈希函数将 (a:1,aaa:100)
// 发送给第一个reducer ，剩下的 发送给第二个reducer
// 注意 这个hash 函数是对数据进行hash 的，这样就保证了第一个mapper 中
// 对应 a 的 出现次数 和第二个 mapper 中对应 a 的出现次数能被
// 同一个 reducer 接收
// 这样 完成 统计后 第一个 reducer 持有 (a, aaa) 的全部统计结果
// 第二个reducer 持有其余单词的统计结果，这样merge后得到了统计结果

func (mr *Master) run(jobName string, files []string, nreduce int,
	schedule func(phase jobPhase),
	finish func(),
) {
	mr.jobName = jobName
	mr.files = files
	mr.nReduce = nreduce

	fmt.Printf("%s: Starting Map/Reduce task %s\n", mr.address, mr.jobName)

	schedule(mapPhase)
	schedule(reducePhase)
	finish()
	mr.merge()

	fmt.Printf("%s: Map/Reduce task completed\n", mr.address)

	mr.doneChannel <- true
}

// Wait blocks until the currently scheduled work has completed.
// This happens when all tasks have scheduled and completed, the final output
// have been computed, and all workers have been shut down.
func (mr *Master) Wait() {
	<-mr.doneChannel
}

// killWorkers cleans up all workers by sending each one a Shutdown RPC.
// It also collects and returns the number of tasks each worker has performed.
func (mr *Master) killWorkers() []int {
	mr.Lock()
	defer mr.Unlock()
	ntasks := make([]int, 0, len(mr.workers))
	for _, w := range mr.workers {
		debug("Master: shutdown worker %s\n", w)
		var reply ShutdownReply
		ok := call(w, "Worker.Shutdown", new(struct{}), &reply)
		if ok == false {
			fmt.Printf("Master: RPC %s shutdown error\n", w)
		} else {
			ntasks = append(ntasks, reply.Ntasks)
		}
	}
	return ntasks
}
