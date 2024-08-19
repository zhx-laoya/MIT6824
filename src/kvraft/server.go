package kvraft

import (
	"bytes"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"github.com/fatih/color"
)

const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	red := color.New(color.FgRed).SprintFunc()
	if Debug > 0 {
		msg := fmt.Sprintf(format, a...)
		fmt.Print(red(msg))
	}
	return
}

func DPrintfR(format string, a ...interface{}) (n int, err error) {
	red := color.New(color.FgRed).SprintFunc()
	if Debug > 0 {
		msg := fmt.Sprintf(format, a...)
		fmt.Print(red(msg))
	}
	return
}
func DPrintB(format string, a ...interface{}) (n int, err error) {
	red := color.New(color.FgBlue).SprintFunc()
	if Debug > 0 {
		msg := fmt.Sprintf(format, a...)
		fmt.Print(red(msg))
	}
	return
}

const (
	GetStirng    = "Get"
	PutString    = "Put"
	AppendString = "Append"
)

type Op struct {
	Commandop string // get/put/append
	Key       string
	Value     string
	//通过clerkid和commandid来唯一标识一条命令
	Clerkid   int
	Commandid int
	Err       string
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type KVServer struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	dead         int32       // set by Kill()
	maxcommandid map[int]int //已知的每个客户端提交的命令的最大id
	//每条rpc请求 等待状态机应用op后提交到数据库，然后才能写reply参数
	waitchan     map[int]chan Op   //int为这条唯一的命令标识符，放入的op为未执行前，拿出的op为执行后
	database     map[string]string //数据库，即状态机
	maxraftstate int               // snapshot if log grows this big
	persister    *raft.Persister
	LastLogIndex int //快照的最后一个日志的下标
	LastLogTerm  int //快照的最后一个日志的任期
	// Your definitions here.
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	//重复发送但已经应用到状态机的命令
	kv.mu.Lock()
	if args.Commandid <= kv.maxcommandid[args.Clerkid] {
		value, ok := kv.database[args.Key]
		kv.mu.Unlock()
		if ok {
			reply.Err = OK
			reply.Value = value
		} else {
			reply.Err = ErrNoKey
			reply.Value = ""
		}
		return
	}
	kv.mu.Unlock()
	//未应用到状态机上的命令
	op := Op{
		Commandop: GetStirng,
		Key:       args.Key,
		Value:     "",
		Clerkid:   args.Clerkid,
		Commandid: args.Commandid,
		Err:       "",
	}
	_, _, is_leader := kv.rf.Start(op)
	if !is_leader {
		reply.Err = ErrWrongLeader
		reply.Value = ""
		return
	}
	// fmt.Printf("GET 我是leader:%v，我的任期：%v，发送的key为:%v,commandid为:%v/%v\n", kv.me, kv.rf.CurrentTerm, args.Key,
	// args.Clerkid%100,args.Commandid)
	channel := make(chan Op, 1)
	kv.mu.Lock()
	//通过clerid^commanid来唯一标识一条命令
	kv.waitchan[args.Clerkid^args.Commandid] = channel
	kv.mu.Unlock()

	select {
	case op1 := <-channel:
		kv.mu.Lock()
		delete(kv.waitchan, args.Clerkid^args.Commandid)
		kv.mu.Unlock()
		if op.Clerkid == op1.Clerkid && op.Commandid == op1.Commandid {
			if op1.Err == ErrNoKey {
				reply.Err = ErrNoKey
			} else {
				reply.Err = OK
			}
			reply.Value = op1.Value
		} else {
			log.Fatal("提交的op不一致")
		}
	case <-time.After(time.Millisecond * 500):
		kv.mu.Lock()
		delete(kv.waitchan, args.Clerkid^args.Commandid)
		kv.mu.Unlock()
		reply.Err = ErrTimeOut
		reply.Value = ""
	}

}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	//重复发送但已经应用到状态机的命令
	kv.mu.Lock()
	if args.Commandid <= kv.maxcommandid[args.Clerkid] {
		// fmt.Println(args.Commandid,kv.maxcommandid[args.Clerkid],args.Value)
		kv.mu.Unlock()
		reply.Err = OK
		return
	}
	kv.mu.Unlock()
	// fmt.Printf("%v-%v\n",args.Clerkid,args.Commandid)
	//未应用到状态机上的命令
	op := Op{
		Commandop: args.Op,
		Key:       args.Key,
		Value:     args.Value,
		Clerkid:   args.Clerkid,
		Commandid: args.Commandid,
	}
	_, _, is_leader := kv.rf.Start(op)
	if !is_leader {
		reply.Err = ErrWrongLeader
		return
	}
	channel := make(chan Op, 1)
	kv.mu.Lock()
	kv.waitchan[args.Clerkid^args.Commandid] = channel
	kv.mu.Unlock()
	select {
	case op1 := <-channel:
		kv.mu.Lock()
		delete(kv.waitchan, args.Clerkid^args.Commandid)
		kv.mu.Unlock()
		if op.Clerkid == op1.Clerkid && op.Commandid == op1.Commandid {
			reply.Err = OK
		} else { //一定是
			t := op1.Err
			if t != ErrRepeatCommit {
				log.Fatal("一定为重复提交")
			}
			reply.Err = Err(op1.Err)
		}
	case <-time.After(time.Millisecond * 500):
		kv.mu.Lock()
		delete(kv.waitchan, args.Clerkid^args.Commandid)
		kv.mu.Unlock()
		reply.Err = ErrTimeOut
	}
}

// 处理所有的命令
func (kv *KVServer) applierhandle() {
	for !kv.killed() {
		m := <-kv.applyCh
		if m.CommandValid { //提交了条目
			//取出提交的命令
			kv.mu.Lock()
			op := m.Command.(Op)
			// fmt.Printf("我是%v，我的身份%v----------------%v-op.command:%v --- kv.maxcommandid:%v\n",
			// kv.me,kv.rf.StatusType,op.Clerkid%100,op.Commandid,kv.maxcommandid[op.Clerkid])
			//可能会出现apply到一半
			//几条连续的相同提交出现，这里不要往waitchan放入op了,这里不用放进通道，不然会阻塞通道
			//因为通道被delete，再往里面塞就会阻塞。
			if op.Commandid < kv.maxcommandid[op.Clerkid]+1 {
				// fmt.Printf("%v ----- %v\n", op.Commandid, kv.maxcommandid[op.Clerkid])
				// log.Fatal("发起调用顺序不对")x
				op.Err = ErrRepeatCommit
				kv.mu.Unlock()
				continue
			}
			//get命令
			if op.Commandop == GetStirng {
				value, ok := kv.database[op.Key]
				if ok {
					op.Value = value
					kv.maxcommandid[op.Clerkid] += 1
					// DPrintfR("%v 提交了Get：%v\n", op.Clerkid%100, value)
				} else { //没有这个key
					op.Value = ""
					op.Err = ErrNoKey
					kv.maxcommandid[op.Clerkid] += 1
				}
			} else if op.Commandop == PutString {
				kv.database[op.Key] = op.Value
				kv.maxcommandid[op.Clerkid] += 1
			} else { //append
				kv.database[op.Key] += op.Value
				// DPrintfR("%v Append了：%v ,提交完后为:%v\n", op.Clerkid%100, op.Value,kv.database[op.Key])
				kv.maxcommandid[op.Clerkid] += 1
			}
			//状态机应用完毕
			//unlock必须放在下面防止死锁
			//如果是leader则需要将这个op传入通道
			if _, ok := kv.waitchan[op.Clerkid^op.Commandid]; ok {
				kv.waitchan[op.Clerkid^op.Commandid] <- op
			}
			//解决snapshot问题
			if kv.maxraftstate != -1 && kv.persister.RaftStateSize() > kv.maxraftstate { //超过最大快照长度则进行快照
				w := new(bytes.Buffer)
				e := labgob.NewEncoder(w)
				e.Encode(m.CommandIndex)
				//把数据库快照出来
				e.Encode(kv.database)
				e.Encode(kv.maxcommandid)
				kv.rf.Snapshot(m.CommandIndex, w.Bytes())
			}
			kv.mu.Unlock()

		} else if m.SnapshotValid { //follower提交上来的snapshot应该把它直接应用到状态机上面来
			kv.applysnapshot(m.Snapshot)
		} else {
			log.Fatal("无效的提交")
		}
	}
}

func (kv *KVServer) applysnapshot(snapshot []byte) { //将快照应用到状态机上
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.maxraftstate == -1 {
		return
	}
	if snapshot == nil {
		log.Fatal("空的 snapshot")
		return
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var lastIncludedIndex int
	var database map[string]string
	var maxcommandid map[int]int
	if d.Decode(&lastIncludedIndex) != nil ||
		d.Decode(&database) != nil ||
		d.Decode(&maxcommandid) != nil {
		log.Fatal("snapshot decode 错误")
		return
	}
	kv.LastLogIndex = lastIncludedIndex
	kv.database = database
	kv.maxcommandid = maxcommandid
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
// 当不再需要一个 KVServer 实例时，测试程序会调用 Kill() 方法。
// 为了方便起见，我们提供了设置 rf.dead 的代码（无需锁定），
// 以及一个用于在长时间运行的循环中测试 rf.dead 的 killed() 方法。
// 你也可以添加自己的代码到 Kill() 方法中。
// 对于这一点，你并不需要做任何事情，但这可能很方便（例如）
// 用于禁止来自已调用 Kill() 的实例的调试输出。
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
// servers[] 包含一组将通过 Raft 合作以构建容错的键值服务的服务器端口。
// me 是当前服务器在 servers[] 中的索引。
// 键值服务器应通过底层 Raft 实现存储快照，该实现应调用 persister.SaveStateAndSnapshot()
// 来原子性地保存 Raft 状态以及快照。
// 当 Raft 的保存状态超过 maxraftstate 字节时，键值服务器应进行快照，
// 以便让 Raft 对其日志进行垃圾回收。如果 maxraftstate 为 -1，
// 则不需要进行快照。
// StartKVServer() 必须迅速返回，因此应该为任何长时间运行的任务启动 goroutines。
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	// 在你希望 Go 的 RPC 库进行编组/解组的结构体上调用 labgob.Register。
	labgob.Register(Op{})
	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.maxcommandid = make(map[int]int)
	kv.waitchan = make(map[int]chan Op)
	kv.database = make(map[string]string)
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.persister = persister
	// You may need initialization code here.
	if len(kv.persister.ReadSnapshot()) != 0 {
		kv.applysnapshot(kv.persister.ReadSnapshot())
	}
	go kv.applierhandle()
	return kv
}
