package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"fmt"
	"log"
	"math/rand"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
)

// import "bytes"
// import "../labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
// ApplyMsg, but set CommandValid to false for these other uses.
// 当每个 Raft 对等节点意识到连续的日志条目已经被提交时，
// 对等节点应通过传递给 Make() 的 applyCh 向服务（或测试程序）发送 ApplyMsg。
// 将 CommandValid 设置为 true，以指示 ApplyMsg 包含一个新提交的日志条目。

// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// 在实验三中，您可能希望在 applyCh 上发送其他类型的消息（例如快照）；
// 在这种情况下，您可以向 ApplyMsg 添加字段，但对于这些其他用途，请将 CommandValid 设置为 false。
type DebugMutex struct {
	sync.Mutex
	name             string
	lastLock         time.Time
	lockingGoroutine string
	stacktrace       string
}

// 每秒检测锁的持有时间
func (m *DebugMutex) CheckLocktime() {
	for {
		// pc, file, line, _ := runtime.Caller(1)
		// lockPosition := fmt.Sprintf("%s:%d %s", file, line, runtime.FuncForPC(pc).Name())
		if time.Since(m.lastLock) > 5*time.Second { //持久时间超过一秒的锁
			// fmt.Printf("Locked %s at %s\n", m.name, lockPosition)
			// break
			fmt.Printf("就是你占用了锁！！！,%s\n你的位置：%s\n", m.lockingGoroutine, m.stacktrace)
			fmt.Printf("\n\n\n")
			// break
		}
		time.Sleep(time.Second)
	}
}
func (m *DebugMutex) Lock() {
	m.Mutex.Lock()
	m.lastLock = time.Now()
	// 获取当前 goroutine 的 ID
	buf := make([]byte, 64)
	n := runtime.Stack(buf, false)
	idField := bytes.Fields(buf[:n])[1]
	goroutineID := string(idField)

	// 获取调用栈
	stackBuf := make([]byte, 4096)
	stackSize := runtime.Stack(stackBuf, false)
	stackTrace := string(bytes.TrimRight(stackBuf[:stackSize], "\n"))

	// 存储 goroutine ID 和调用栈
	m.lockingGoroutine = goroutineID
	m.stacktrace = stackTrace

	// 可选：打印锁定信息
	// fmt.Printf("Locked %s by goroutine %s\n", m.name, goroutineID)
}

func (m *DebugMutex) Unlock() {
	duration := time.Since(m.lastLock)
	if duration > 100*time.Millisecond {
		fmt.Printf("Unlocking %s after %v (locked at %s)\n", m.name, duration, m.lastLock)
	}
	m.Mutex.Unlock()
}

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

const (
	follower_type  = 0
	candidate_type = 1
	leader_type    = 2
)

// A Go object implementing a single Raft peer.
// 存的是单个服务器
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers 全局的所有服务器包括自己
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[] //自己服务器所在下标， peers[me]
	dead      int32               // set by Kill()
	// applyCh 是一个通道，测试程序或服务希望 Raft 通过该通道发送 ApplyMsg 消息。
	StatusType int //当前服务器的身份
	Applych    chan ApplyMsg
	//lab2A
	CurrentTerm   int       //接收到的最大的一个任期
	VotedFor      int       //当前的投票信息 ，如果为len(peers)则为未投票，
	ElectionTime  time.Time //选举超时时间
	HeartbeatTime time.Time //心跳超时时间
	votechan      chan bool //投票通道
	Log           []Entry   //日志条目，第一个日志的下标为1
	//lab2B
	CommitIndex int //已知最高提交条目的下标，最开始为0表示没有提交
	LastApplied int //最高的应用到状态机的条目下标,即发送到applychan的最高下标

	//这两个在成为leader时都要初始化

	//初始化为leader的最后一个条目的下标+1
	NextIndex []int //leader才有，要发送给每个服务器的下一个条目下标（指的是leader的下标），

	//初始化为0
	MatchIndxt []int //leader才有，每个服务器已经复制的条目下标（指的是其他服务器的下标）
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	//2D
	lastIncludedIndex int
	lastIncludedTerm  int
	currentsnapshot   []byte
}
type Entry struct {
	Command interface{} //提交到状态机上面的命令，也就是客户端的请求
	Term    int         //该命令是在哪个任期被leader接收到的
	Index   int         //该命令的下标
}

// return currentTerm and whether this server
// believes it is the leader.
// 返回当前服务器的状态
func (rf *Raft) GetState() (int, bool) {
	//这里没加锁导致-race出问题了
	rf.mu.Lock()
	defer rf.mu.Unlock()
	var term int
	var isleader bool
	term = rf.CurrentTerm
	isleader = (rf.StatusType == leader_type)
	// fmt.Printf("我的id：%v，我的身份：%v\n 我的任期：%v",rf.me,rf.StatusType,rf.CurrentTerm)
	// Your code here (2A).
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// 在响应rpc之前更新了稳定存储
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	//编码
	e.Encode(rf.CurrentTerm)
	e.Encode(rf.VotedFor)
	e.Encode(rf.Log)
	e.Encode(rf.lastIncludedTerm)
	e.Encode(rf.lastIncludedIndex)
	data := w.Bytes()
	rf.persister.SaveStateAndSnapshot(data, rf.currentsnapshot)
	// DPrintB("我的id是：%v ----- ",rf.me)
	// DPrintB("我的日志是：%v\n",rf.Log)
}

// restore previously persisted state.
// 恢复以前保存的状态。
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var current_term int
	var votefor int
	var log []Entry
	var lastIncludedTerm int
	var lastIncludedIndex int
	if d.Decode(&current_term) != nil ||
		d.Decode(&votefor) != nil || d.Decode(&log) != nil ||
		d.Decode(&lastIncludedTerm) != nil || d.Decode(&lastIncludedIndex) != nil {
		DPrintB("持久化读取出错了")
	} else {
		rf.mu.Lock()
		rf.CurrentTerm = current_term
		rf.VotedFor = votefor
		rf.Log = log
		rf.lastIncludedTerm = lastIncludedTerm
		rf.lastIncludedIndex = lastIncludedIndex
		rf.currentsnapshot = rf.persister.ReadSnapshot()
		rf.mu.Unlock()
	}
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
// 服务切换到快照，只有当Raft没有更多的最新信息时才这样做，因为它在applyCh上传递快照
// 直接return true不用写代码
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).
	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
// 服务器说它已经创建了一个到达including index的快照，意味着服务器不再
// 需要小于这个索引的日志了。
// Raft 现在应该尽可能地修剪其日志
// 由config设置为每10条日志进行一次snapshot
// snapshot包含lastincludeindex和从1到lastincludeidex所有的日志
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	pos := 0
	for i := 1; i < len(rf.Log); i++ {
		if rf.Log[i].Index <= index {
			pos = i
		}
	}
	// fmt.Printf("%v snapshot了：%v\n", rf.me, index)
	//裁剪
	if pos != 0 {
		rf.Log = rf.Log[pos:]
		rf.lastIncludedIndex = rf.Log[0].Index
		rf.lastIncludedTerm = rf.Log[0].Term
		rf.currentsnapshot = snapshot
	}
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!字段名必须以大写字母开头!
// 由candidate发送
type RequestVoteArgs struct {
	//2A 用于candidate发送投票请求
	Term         int //candidate的当前任期
	CandidateId  int //发送者的id
	LastLogIndex int //candidate的最后一个entry的下标
	LastLogTerm  int //candidate的最后一个entry的任期
	// Your data here (2A, 2B).
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
// 由其他服务器回复
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // 恢复者的term信息
	VoteGranted bool //true代表赞同票
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	//把resettime的加锁操作去掉
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//已知candidate的信息存储在args中
	reply.Term = rf.CurrentTerm
	servers := len(rf.peers)

	//reply false if term <currentterm
	if args.Term < rf.CurrentTerm { //过期rpc 不管,但也要返回我的任期给candidate知道
		reply.VoteGranted = false
		return
	}
	// fmt.Printf("收到request信息了！！！，我的id：%v，发送方任期：%v,我的任期：%v 收到信息后我的身份：%v\n", rf.me, args.Term, rf.CurrentTerm, rf.StatusType)
	//rules for servers - all serve - 2
	if args.Term > rf.CurrentTerm {
		// if rf.StatusType == leader_type {
		// 	fmt.Printf("我是leader:%d,我收到requesetvote消息了\n", rf.me)
		// }
		rf.CurrentTerm = args.Term
		rf.changeto(follower_type)
	}

	//requesetrpc - 2
	//由于我的args写错了,调试过程中出现没投票的情况
	if rf.VotedFor == servers || rf.VotedFor == args.CandidateId {
		// 候选人的日志至少与接收者的日志一样最新
		// 如果两者最近的日志term号不同，那么越大的越新，如果term号一样，越长的日志（拥有更多entries）越新
		// fmt.Printf("----------- %v %v %v %v\n", args.LastLogIndex,
		//  len(rf.Log)-1, args.LastLogTerm, rf.Log[len(rf.Log)-1].Term)
		//term更大
		if args.LastLogTerm > rf.Log[len(rf.Log)-1].Term { //只有投票才重置超时时间
			reply.VoteGranted = true
			rf.VotedFor = args.CandidateId
			//投同意票要重置超时器
			rf.reseteletiontime()
			// DPrintB("%v(任期：%v）给%v（任期%v）投票了\n", rf.me, rf.CurrentTerm, args.CandidateId, args.Term)
		} else if args.LastLogTerm == rf.Log[len(rf.Log)-1].Term { //term一样且更长
			if args.LastLogIndex >= rf.Log[len(rf.Log)-1].Index {
				reply.VoteGranted = true
				rf.VotedFor = args.CandidateId
				//投同意票要重置超时器
				rf.reseteletiontime()
				// DPrintB("%v(任期：%v）给%v（任期%v）投票了\n", rf.me, rf.CurrentTerm, args.CandidateId, args.Term)
			}
		}
	}
	rf.persist()
	//响应rpc持久化
	// // 正确的逻辑
	// if rf.VotedFor == servers {
	// 	reply.VoteGranted = true
	// 	rf.VotedFor = args.CandidateId
	// 	rf.reseteletiontime()
	// } else if rf.VotedFor == args.CandidateId { //已经投票并且日志更新
	// 	if args.LastLogIndex >= len(rf.Log)-1 && args.LastLogTerm >= rf.Log[len(rf.Log)-1].Term {
	// 		reply.VoteGranted = true
	// 		rf.VotedFor = args.CandidateId
	// 		//投同意票要重置超时器
	// 		rf.reseteletiontime()
	// 	}
	// }

}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[]. server 是目标服务器在 rf.peers[] 中的索引。
// expects RPC arguments in args. 期望将 RPC 参数放入 args 中。
// fills in *reply with RPC reply, so caller should 使用 RPC 回复填充 *reply，因此调用方应传递 &reply
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// labrpc 包模拟了一个有丢失的网络，在该网络中，服务器可能无法访问，请求和回复可能会丢失。
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// Call() 发送请求并等待回复。如果在超时间隔内收到回复，Call() 将返回 true；
// 否则，Call() 将返回 false。因此，Call() 可能要等一段时间才会返回。
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
// 返回 false 可能是由于服务器宕机、无法访问的活动服务器、丢失的请求或丢失的回复引起的。
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
//	Call() 保证会返回（可能会有延迟），除非服务器端的处理函数没有返回。
//	因此，在 Call() 周围没有必要实现自己的超时机制
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
// 由candidate发起
// 注意这里发给对面的时候对面可能会死锁了！！！！！！！！！！！
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	rf.mu.Lock()
	rf.persist()
	rf.mu.Unlock()
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// AppendEntirs RPC
// 由leader发起
type AppendEntriesArgs struct {
	Term         int //leader term
	LeaderId     int
	PrevLogIndex int     //紧接在新条目之前的日志条目的索引，为了其他服务器的条目是否和我的一样
	PreLogTerm   int     //prevlog的任期
	Entries      []Entry //empty为心跳，可能会发送多条日志
	LeaderCommit int     //leader的commitindex
}
type AppendEntriesReply struct {
	Term          int  //返回当前服务器的currentterm
	Success       bool //true表明服务正常
	ConflictTerm  int
	ConflictIndex int
}

func min(a, b int) int {
	if a < b {
		return a
	} else {
		return b
	}
}

// 返回等于index的位置，exist返回这个下标是否存在
func (rf *Raft) getindex(index int) (int, bool) {
	l, r := 0, len(rf.Log)-1
	for l < r {
		mid := (l + r) / 2
		if rf.Log[mid].Index < index {
			l = mid + 1
		} else {
			r = mid
		}
	}
	if rf.Log[l].Index == index {
		return l, true
	} else {
		return -1, false
	}
}
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.CurrentTerm
	reply.Success = true
	// fmt.Printf("我是：%d，我的身份：%v，我收到leader消息了！我的任期:%v,leader任期：%v\n", rf.me, rf.StatusType, rf.CurrentTerm, args.Term)
	// fmt.Printf("发送者任期：%v，接收者任期：%v\n",args.Term,reply.Term)
	//图二append逻辑，更新的是日志相关信息
	// 1 任期大于发送者任期
	if args.Term < rf.CurrentTerm { //leader发送的任期比它已知任期小 ，则表明该服务器拒绝该append请求
		reply.Success = false
		return
	}
	preindex, exist := rf.getindex(args.PrevLogIndex)
	if !exist && args.PrevLogIndex < rf.Log[0].Index {
		// fmt.Printf("args.prevlogindex:%v ----- preindex")
		// 小于提交下标将他当作是过期的rpc即可
		reply.Success = false
		return
		// log.Fatal("不可能小于提交下标\n")
	}

	//收到现任leader的请求
	rf.reseteletiontime()
	//2 rf没有prevlogindex 或者 在prevlogindex下的条目的任期与发送者不一样，false
	// 这里会出现一个bug，任期不一样但是要相当于接受到心跳,在TestBackup2B里面会发送50次，也就是返回50次失败
	// 这样心跳就没更新了，导致leader没有发送心跳,所以要更新选举时间
	//false 的时候需要这样去找nextindex

	if args.PrevLogIndex >= rf.Log[len(rf.Log)-1].Index+1 || (exist && rf.Log[preindex].Term != args.PreLogTerm) {
		// fmt.Printf("-----------------------------------------发送者id:%v(prelogindex:%v),接收者id：%v,接收者日志 ", args.LeaderId, args.PrevLogIndex, rf.me)
		// for i := 1; i < len(rf.Log); i++ {
		// 	fmt.Printf("%v(%v)/%v ", rf.Log[i].Term, rf.Log[i].Index, rf.Log[i].Command.(int)%1000)
		// }
		// fmt.Printf("\n")
		//优化nextindex
		if rf.Log[len(rf.Log)-1].Index+1 <= args.PrevLogIndex { //下一次直接从尾部发送
			reply.ConflictTerm = -1
			reply.ConflictIndex = rf.Log[len(rf.Log)-1].Index + 1
		} else {
			reply.ConflictTerm = rf.Log[preindex].Term
			//找到第一个为conflictterm的位置
			for i := 1; i <= preindex; i++ {
				if rf.Log[i].Term == reply.ConflictTerm {
					reply.ConflictIndex = rf.Log[i].Index
					break
				}
			}
		}
		reply.Success = false
		return
	}
	//3 如果在要填入的下标下有条目且与args的条目不一致（same index but different term)
	//则删除从这个下标往后的的所有条目，包括该下标,注意比较的是发送过来的第一个元素
	//心跳并不能判断是否要删除东西！！！！！
	//如果没有进入这块截断的且follower的长度比leader的长度长的话，那很可能出错，比如多的东西没切掉
	// if args.PrevLogIndex+1 < len(rf.Log) { //有条目
	// 	if args.Entries != nil && args.Entries[0].Term != rf.Log[args.PrevLogIndex+1].Term { //任期不一致
	// 		rf.Log = rf.Log[:args.PrevLogIndex+1] //开区间，只保留[0,args.prevLogIndex]的条目
	// 	}
	// }
	//4 添加发送者发来的条目 ，这里不能直接append所有，而是append any new entires not already in the log
	if args.Entries != nil { //
		index, _ := rf.getindex(args.PrevLogIndex + 1)
		if index == -1 {
			index = len(rf.Log)
		}
		for i := 0; i < len(args.Entries); i++ {
			// index := args.PrevLogIndex + 1 + i
			if index < len(rf.Log) { //原本就有这个下标
				if args.Entries[i] != rf.Log[index] { //存在一个不一样的则把它后面包括它全都去掉
					rf.Log = rf.Log[:index]
					//同时把自己加进来
					rf.Log = append(rf.Log, args.Entries[i])
				}
			} else { //超过log长度直接append
				rf.Log = append(rf.Log, args.Entries[i])
			}
			index++
		}
	}

	// fmt.Printf("我是%v:",rf.me)
	// for i:=0;i<len(rf.Log);i++ {
	// 	fmt.Printf("%v/%v ",rf.Log[i].Term,rf.Log[i].Index)
	// }
	// fmt.Print("\n")
	//因为心跳不能判断是否要删除东西，所以不能在这里提交，否则可能会提交leader没有提交的东西
	//5 如果leaderCommit>commitIndex,set commitindex=min(leaderCommit,index of last new entry)
	if args.Entries == nil && args.LeaderCommit > rf.CommitIndex { //发送心跳
		//这里的commit只能到prelogindex ,因为心跳并不保证后面的东西已经复制上了，只能代表到prelogindex是完全一样的
		//如果不加这个的话，很大概率会出现发送空的心跳包后，follower的commitindex变得很大，然而东西还没复制上去
		//就导致了apply error
		rf.CommitIndex = min(args.LeaderCommit, rf.Log[len(rf.Log)-1].Index)
		rf.CommitIndex = min(rf.CommitIndex, args.PrevLogIndex)
		// go rf.applytomachine(rf.Applych)
	} else if args.Entries != nil && args.LeaderCommit > rf.CommitIndex {
		//发送的leadercommit比previndex要大，难受了
		rf.CommitIndex = min(args.LeaderCommit, rf.Log[len(rf.Log)-1].Index)
		// go rf.applytomachine(rf.Applych)
		// fmt.Printf("我是%v，我的commitindex更新为%v ---- ",rf.me,rf.CommitIndex)
		// fmt.Printf("%v 给 %v 发送信息且prelogindex为%v,发送完后我的commitindex:%v,----",args.LeaderId,rf.me,args.PrevLogIndex,rf.CommitIndex)
		// fmt.Printf("  发送的日志为：\n")
		// for i:=0;i<len(args.Entries);i++ {
		// 	fmt.Printf("%v/%v ",args.Entries[i].Term,args.Entries[i].Command)
		// }
		// fmt.Printf("\n")
		// rf.CommitIndex = min(rf.CommitIndex, args.PrevLogIndex) //只能自己加上这个来保证一致性了
	}

	//rules servers-candidate -4:如果收到leader的append信息，转变为follower
	if args.Entries != nil && rf.StatusType == candidate_type {
		rf.changeto(follower_type)
		rf.CurrentTerm = args.Term
	}
	if args.Entries == nil { //心跳
		rf.reseteletiontime() //重置超时时间
	}
	//rule servers-allservers-2 更新状态信息
	//只可能我是candidate或者follower,那么接收请求即将我转变为follower,注意votefor不会变化
	if args.Term > rf.CurrentTerm {
		rf.changeto(follower_type)
		rf.CurrentTerm = args.Term
	}
	rf.persist()
}
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	rf.mu.Lock()
	rf.persist()
	rf.mu.Unlock()
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

type InstallSnapshotargs struct {
	Term              int //leader的任期
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte //快照信息
}
type InstallSnapshotReply struct {
	Term int //返回它的任期让leader更新
}

func (rf *Raft) leader_sendsnapshot() {
	// fmt.Printf("我成为leader了，我的任期：%v，我的id：%v\n",rf.CurrentTerm,rf.me)
	rf.mu.Lock()
	if rf.currentsnapshot == nil {
		rf.mu.Unlock()
		return
	}
	me := rf.me
	servers := len(rf.peers)
	term := rf.CurrentTerm
	rf.mu.Unlock()
	// fmt.Printf("我是leader:%v,我进入leader_do了\n",rf.me)
	var wg sync.WaitGroup
	for i := 0; i < servers; i++ {
		rf.mu.Lock()
		statustype := rf.StatusType
		rf.mu.Unlock()

		if i != me && statustype == leader_type {
			wg.Add(1)
			go func(server int) {
				// fmt.Printf("%v 打算向 %v 发送append消息\n",rf.me,server)
				defer wg.Done()
				rf.mu.Lock()
				if rf.StatusType != leader_type {
					rf.mu.Unlock()
					return
				}
				if rf.CurrentTerm != term {
					rf.mu.Lock()
					return
				}
				args := InstallSnapshotargs{}
				reply := InstallSnapshotReply{}
				args.Term = rf.CurrentTerm
				args.LeaderId = rf.me
				args.LastIncludedTerm = rf.lastIncludedTerm
				args.LastIncludedIndex = rf.lastIncludedIndex
				args.Data = rf.currentsnapshot

				//发送前要判断一次
				if rf.StatusType != leader_type {
					rf.mu.Unlock()
					return
				}
				rf.mu.Unlock()

				ok := rf.sendInstallSnapshot(server, &args, &reply)
				if !ok {
					return
				}

				//check
				rf.mu.Lock()
				if reply.Term > term { //其他节点的任期较大
					rf.changeto(follower_type)
					rf.persist()
					rf.mu.Unlock()
					return
				}
				if rf.CurrentTerm != term {
					rf.mu.Unlock()
					return
				}
				rf.mu.Unlock()
			}(i)
		}
	}
	wg.Wait()
}
func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotargs, reply *InstallSnapshotReply) bool {
	rf.mu.Lock()
	rf.persist()
	rf.mu.Unlock()
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

// 逻辑 leader发送snapshot过来，follower将这个snapshot传入我的applymsg中
func (rf *Raft) InstallSnapshot(args *InstallSnapshotargs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	reply.Term = rf.CurrentTerm
	if args.Term < rf.CurrentTerm {
		rf.mu.Unlock()
		return
	}
	if args.Term>rf.CurrentTerm {
		rf.changeto(follower_type)
	}
	if args.LastIncludedIndex <= rf.LastApplied { //发送的
		rf.mu.Unlock()
		return
	}
	//2，3 直接写数据到snapshot中
	rf.currentsnapshot = args.Data
	//切掉lastincludeindx以及之前的日志
	for i := 1; i < len(rf.Log); i++ {
		if rf.Log[i].Index == args.LastIncludedIndex && rf.Log[i].Term == args.LastIncludedTerm {
			rf.Log = rf.Log[i:]
			rf.lastIncludedIndex = args.LastIncludedIndex
			rf.lastIncludedTerm = args.LastIncludedTerm
			msg := ApplyMsg{
				CommandValid:  false,
				SnapshotValid: true,
				Snapshot:      args.Data,
				SnapshotTerm:  args.LastIncludedTerm,
				SnapshotIndex: args.LastIncludedIndex,
			}
			// fmt.Printf("%v快照了%v\n",rf.me,rf.lastIncludedIndex)
			rf.mu.Unlock()
			rf.Applych <- msg
			return
		}
	}

	//不存在一致则切掉整个,即留下0号
	rf.Log = rf.Log[0:1]
	rf.Log[0].Index = args.LastIncludedIndex
	rf.Log[0].Term = args.LastIncludedTerm
	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm
	DPrintB("我是%v,我的任期：%v，我的身份：%v，我切到了:%v\n", rf.me, rf.CurrentTerm, rf.StatusType, rf.Log[0].Index)
	msg := ApplyMsg{
		CommandValid:  false,
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIndex,
	}
	// fmt.Printf("%v快照了%v\n",rf.me,rf.lastIncludedIndex)
	rf.mu.Unlock()
	rf.Applych <- msg
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
// 当服务（例如 k/v 服务器）使用 Raft 时，希望开始就下一个要追加到 Raft 日志中的命令达成一致。
// 如果该服务器不是 leader，则返回 false。否则，开始达成一致并立即返回。
// 不能保证这个命令一定会被提交到 Raft 日志中，
// 因为 leader 可能会失败或在选举中失去领导地位。即使 Raft 实例已经被关闭，该函数也应该正常返回。

// 第一个返回值是命令如果被提交，将出现的索引。
// 第二个返回值是当前的任期。
// 第三个返回值为 true 表示该服务器认为自己是 leader。
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index := rf.Log[len(rf.Log)-1].Index + 1 //下一个要提交到的下标
	term := rf.CurrentTerm
	isLeader := (rf.StatusType == leader_type)
	// Your code here (2B).
	//不是leader直接返回false
	if !isLeader {
		return index, term, isLeader
	}
	//是leader得让leader存储这个命令
	//构建一个条目
	entry := Entry{
		Command: command,
		Term:    term,
		Index:   index,
	}
	rf.Log = append(rf.Log, entry)
	// log.Println(rf.Log)
	return index, term, isLeader
}

//
// atomic 包提供了原子操作的函数，用于在多个goroutine之间进行原子操作。
// 这些原子操作可以保证操作的原子性，即在执行期间不会被中断。
// atomic 包的函数使用底层的原子指令来确保操作的原子性，而不需要使用互斥锁

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
// 在每个测试之后，测试程序不会停止 Raft 创建的 goroutine，
// 但它会调用 Kill() 方法。您的代码可以使用 killed() 来检查是否已调用 Kill()。
// 使用 atomic 可以避免使用锁。

// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
// 问题在于长时间运行的 goroutine 会使用内存，并可能占用 CPU 时间，
// 这可能导致后续的测试失败并生成令人困惑的调试输出。
// 任何具有长时间运行循环的 goroutine 应调用 killed() 来检查是否应该停止。
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
// 服务或测试程序希望创建一个 Raft 服务器。所有 Raft 服务器（包括此服务器）的端口都在 peers[] 中，
// 此服务器的端口是 peers[me]。所有服务器的 peers[] 数组顺序相同。
// persister 是用于该服务器保存其持久状态的位置，并且在有时还保存最近保存的状态。
// applyCh 是一个通道，测试程序或服务希望 Raft 通过该通道发送 ApplyMsg 消息。
// Make() 必须快速返回，因此它应为任何长时间运行的工作启动 goroutine。
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.StatusType = follower_type
	rf.Applych = applyCh
	rf.CurrentTerm = 0 //初始化任期为0
	rf.CommitIndex = 0
	rf.LastApplied = 0
	rf.Log = make([]Entry, 0, 10)
	entry := Entry{ //追加一个空的log
		Command: nil,
		Term:    -1,
		Index:   0,
	}
	rf.Log = append(rf.Log, entry)
	rf.NextIndex = make([]int, len(peers))
	//初始化为1
	for i := 0; i < len(peers); i++ {
		rf.NextIndex[i] = 1
	}
	rf.MatchIndxt = make([]int, len(peers))
	// Your initialization code here (2A, 2B, 2C).
	rf.VotedFor = len(rf.peers)
	rf.reseteletiontime()
	rf.votechan = make(chan bool, 5)
	//把持久化的东西读取出来
	// rf.readPersist(persister.raftstate)

	// fmt.Printf("我的id是：%v，我的超时时间是%v\n",rf.me,rf.ElectionTime)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.Ticker(applyCh)
	go rf.applytomachine(applyCh)
	// go rf.mu.CheckLocktime()
	return rf
}

// 应用到状态机
func (rf *Raft) applytomachine(applyCh chan ApplyMsg) {
	for !rf.killed() {
		rf.mu.Lock()
		//它不会提交它不存在的日志，这些日志存在于快照中
		if rf.LastApplied < rf.lastIncludedIndex {
			rf.LastApplied = rf.lastIncludedIndex
			rf.mu.Unlock()
			time.Sleep(time.Millisecond * 10)
			continue
		}
		if rf.CommitIndex <= rf.LastApplied {
			rf.mu.Unlock()
			time.Sleep(time.Millisecond * 10)
			continue
		}
		rf.LastApplied++
		lastindex, _ := rf.getindex(rf.LastApplied)
		if lastindex == -1 {
			fmt.Printf("applytomachine 的下标一定存在\n")
			log.Fatal("applytomachine 的下标一定存在")
		}
		//构造applyms
		msg := ApplyMsg{
			CommandValid: true,
			Command:      rf.Log[lastindex].Command,
			CommandIndex: rf.Log[lastindex].Index,
		}
		// DPrintB("我是%v，我提交了:%v\n",rf.me,msg.Command)
		rf.mu.Unlock()
		applyCh <- msg
	}
}

// 管道是引用类型
func (rf *Raft) Ticker(applyCh chan ApplyMsg) {
	//超时选举检查,follower和candidate超时都重新开始选举
	for !rf.killed() {
		//提交信息到状态机
		// go rf.applytomachine(applyCh)

		rf.mu.Lock()
		if time.Now().After(rf.ElectionTime) && rf.StatusType != leader_type { //则它成为candidate
			rf.changeto(candidate_type)
			go rf.candidate_do()
			// fmt.Printf("我成为candidate了，我的id是：%v，我的任期是：%v，我的超时时间是：%v\n", rf.me, rf.CurrentTerm, rf.ElectionTime)
		}
		//不是在这里发起投票请求的！！！！！，candidate一个任期只会发送一次投票请求
		// if Statustype == candidate_type { //为候选者则发送投票信息
		if rf.StatusType == leader_type { //向其他端口发送append信息,
			go rf.leader_do(false)
			//rules for servers - leaders -4
			//如果存在一个N使得N>commitindex,大多数matchindex[i]>=N
			//并且log[N].Term==currentterm,则set commitindex=N
			go rf.UpdateCommitIndex() //!!!
			//如果心跳超时，发送heartbeat
			if time.Now().After(rf.HeartbeatTime) { //100ms发送一次心跳
				go rf.leader_do(true)
				go rf.leader_sendsnapshot()
				rf.HeartbeatTime = time.Now().Add(time.Duration(100) * time.Millisecond)
			}
		}

		rf.mu.Unlock()

		time.Sleep(time.Duration(10) * time.Millisecond) //20ms检测一次
	}
}
func (rf *Raft) leader_do(is_heartbeat bool) {
	// fmt.Printf("我成为leader了，我的任期：%v，我的id：%v\n",rf.CurrentTerm,rf.me)
	rf.mu.Lock()
	me := rf.me
	servers := len(rf.peers)
	term := rf.CurrentTerm
	rf.mu.Unlock()
	// fmt.Printf("我是leader:%v,我进入leader_do了\n",rf.me)
	var wg sync.WaitGroup
	for i := 0; i < servers; i++ {
		rf.mu.Lock()
		statustype := rf.StatusType
		rf.mu.Unlock()
		if i != me && statustype == leader_type {
			wg.Add(1)
			go func(server int) {
				// fmt.Printf("%v 打算向 %v 发送append消息\n",rf.me,server)
				defer wg.Done()
				rf.mu.Lock()
				if rf.StatusType != leader_type {
					rf.mu.Unlock()
					return
				}
				args := AppendEntriesArgs{}
				args.Term = rf.CurrentTerm
				args.LeaderId = me
				//如果是心跳应该是发送最后一个而不是next的
				// if is_heartbeat {
				// 	args.PrevLogIndex=len(rf.Log)-1
				// 	args.PreLogTerm=rf.Log[len(rf.Log)-1].Term
				// }else {
				//这里可能发送给它已经提交的东西的
				if rf.NextIndex[server] > rf.Log[len(rf.Log)-1].Index+1 {
					fmt.Printf("error 777\n")
					fmt.Printf("我是%v，我的任期：…%v，我的身份：%v : rf.NextIndex[server]:%v -------------- rf.Log[len(rf.Log)-1].Index+1:%v", rf.me, rf.NextIndex[server], rf.CurrentTerm,
					 rf.StatusType, rf.Log[len(rf.Log)-1].Index+1)
					panic("error 777\n")
				}
				if rf.NextIndex[server]-1 < rf.lastIncludedIndex {
					rf.NextIndex[server] = rf.lastIncludedIndex + 1
				}
				if rf.NextIndex[server] > rf.Log[len(rf.Log)-1].Index+1 {
					fmt.Printf("error 666\n")
					fmt.Printf("rf.NextIndex[server]:%v -------------- rf.Log[len(rf.Log)-1].Index+1:%v", rf.NextIndex[server], rf.Log[len(rf.Log)-1].Index+1)
					panic("error 666\n")
				}
				index, exist := rf.getindex(rf.NextIndex[server] - 1)
				if !exist {
					DPrintB("我的id：%v，我的日志：", rf.me)
					for index, entry := range rf.Log {
						if index > 0 {
							DPrintfR("%v(%v)/%v ", entry.Term, entry.Index, entry.Command.(int)%100)
						}
					}
					fmt.Printf("nextindex-1:%v ----- lastindex:%v lastincludeindex:%v\n", rf.NextIndex[server]-1, rf.Log[len(rf.Log)-1].Index, rf.lastIncludedIndex)
					panic("aaaaaaaa\n")
				}
				args.PrevLogIndex = rf.NextIndex[server] - 1
				args.PreLogTerm = rf.Log[index].Term
				args.LeaderCommit = rf.CommitIndex
				args.Entries = nil
				if is_heartbeat { //如果是心跳则发送这个
					args.Entries = nil
				} else { //不是心跳
					//有东西要发送
					if (rf.Log[len(rf.Log)-1].Index) >= rf.NextIndex[server] { //左端点到最后的都赋给它
						args.Entries = rf.Log[index+1:]
						// fmt.Println(args.Entries)
					} else { //没东西发送
						rf.mu.Unlock()
						return
					}
				}
				//发送前要判断一次
				if rf.StatusType != leader_type {
					rf.mu.Unlock()
					return
				}
				rf.mu.Unlock()
				// fmt.Printf("%v向%v发送日志:", rf.me, server)
				// for _, entry := range args.Entries {
				// 	fmt.Printf("%v(%v)/%v ", entry.Term, entry.Index, entry.Command.(int)%1000)
				// }
				// fmt.Printf("\n")
				//如果为nil则是发送心跳，否则是发送信息
				reply := AppendEntriesReply{}
				ok := rf.sendAppendEntries(server, &args, &reply)
				// fmt.Printf("%v向%v发送了一次heartbeat请求,发送任期：%v，接受任期：%v，结果为%v\n", rf.me, server, rf.CurrentTerm, reply.Term, reply.Success)
				if !ok {
					return
				}

				//check
				rf.mu.Lock()
				//all servers - 2
				if reply.Term > term { //其他节点的任期较大
					rf.changeto(follower_type)
					rf.persist()
					rf.mu.Unlock()
					return
				}
				if rf.CurrentTerm != term { //丢掉这次请求
					rf.mu.Unlock()
					return
				}
				if rf.StatusType != leader_type { //发送后再判断一次
					rf.mu.Unlock()
					return
				}
				if args.Entries != nil { //发送信息，否则为发送心跳
					if reply.Success { //发送的信息被接收成功，更新nextindex和matchindex
						rf.MatchIndxt[server] = args.PrevLogIndex + len(args.Entries)
						rf.NextIndex[server] = rf.MatchIndxt[server] + 1
						if rf.NextIndex[server] > rf.Log[len(rf.Log)-1].Index+1 {
							fmt.Printf("error 1\n")
							panic("error 1\n")
						}
					} else { //发送信息失败那就是因为log不一致
						//优化掉-1这种慢的一致性匹配方式
						if reply.ConflictTerm == -1 {
							rf.NextIndex[server] = reply.ConflictIndex
							if rf.NextIndex[server] > rf.Log[len(rf.Log)-1].Index+1 {
								fmt.Printf("error 2\n")
								panic("error 2\n")
							}
						} else {
							conflictIndex := -1
							preindex, _ := rf.getindex(args.PrevLogIndex)
							for i := preindex; i > 0; i-- { //找到最后一个term的位置
								if rf.Log[i].Term == reply.ConflictTerm {
									conflictIndex = rf.Log[i].Index
									break
								}
							}
							if conflictIndex != -1 {
								rf.NextIndex[server] = conflictIndex + 1
								if rf.NextIndex[server] > rf.Log[len(rf.Log)-1].Index+1 {
									fmt.Printf("error 3\n")
									panic("error 3\n")
								}
							} else {
								rf.NextIndex[server] = reply.ConflictIndex
								if rf.NextIndex[server] > rf.Log[len(rf.Log)-1].Index+1 {
									fmt.Printf("error 4\n")
									panic("error 4\n")
								}
							}

						}
					}

				}
				rf.mu.Unlock()
			}(i)
		}
	}
	wg.Wait() // 等待所有 goroutine 完成
}
func (rf *Raft) UpdateCommitIndex() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//if there exists an N such that N > commitindex,a majority of matchindex[i]>=N,
	// and log[N].term == currentTerm: set commitIndex = N
	rf.MatchIndxt[rf.me] = rf.Log[len(rf.Log)-1].Index
	match := make([]int, len(rf.peers))
	copy(match, rf.MatchIndxt)
	sort.Ints(match)
	majority := match[len(rf.peers)/2]
	index, _ := rf.getindex(majority)
	if majority > rf.CommitIndex && rf.Log[index].Term == rf.CurrentTerm {
		rf.CommitIndex = majority
		// go rf.applytomachine(rf.Applych)
	}
}

// candidate要做的事
func (rf *Raft) candidate_do() {
	count := 1
	//给自己投票
	rf.mu.Lock()
	rf.VotedFor = rf.me
	me := rf.me
	servers := len(rf.peers)
	rf.reseteletiontime()
	rf.persist()
	rf.mu.Unlock()
	//开启一轮新的选举
	//发送rpc要使用go协程很大的原因在于发送失败的超时时间很长，同时发送给其他人也会阻塞
	//开启协程来发送信息
	//不开启协程可能出现这种情况（前面只是发现这个问题但不知道为什么，后面调试时发现导致这个的原因是rpc发送失败需要的时间很多，导致消息被堆积在后面了）
	//发送者的身份：1 ，1向2发送了一次请求,发送任期：3，接受任期：6，结果为false
	//然而此时发送者的任期已经不为3了

	//用来确保协程完成
	var wg sync.WaitGroup
	//用来确保count被正确修改
	var countLock sync.Mutex
	for i := 0; i < servers; i++ {
		rf.mu.Lock()
		statustype := rf.StatusType
		rf.mu.Unlock()
		if i != me && statustype == candidate_type {
			wg.Add(1)
			go func(server int) {
				defer wg.Done()
				rf.mu.Lock()
				current_term := rf.CurrentTerm
				args := RequestVoteArgs{}
				args.CandidateId = me
				args.LastLogIndex = rf.Log[len(rf.Log)-1].Index
				args.LastLogTerm = rf.Log[len(rf.Log)-1].Term
				args.Term = rf.CurrentTerm
				reply := RequestVoteReply{}
				rf.mu.Unlock()

				ok := rf.sendRequestVote(server, &args, &reply)
				// fmt.Printf("--------------------------------OUTnum %v \n", num)
				//注意发送成功的时间忽略不计，但是发送失败的时间在600-2000ms之间，这会导致waitgroup阻塞
				//所以检测票数是否大于等于一半时不要等所有协程结束后统一计算，而要一达到要求就进行状态的转换
				if !ok { //发送失败
					return
				}
				// fmt.Printf(" %d(任期为:%v)------发送给------%d(任期为:%v) 结果为:%v\n", rf.me,rf.CurrentTerm, server,reply.Term,reply.VoteGranted)
				// DPrintf("发送者的日志最后一个term:%v\n",rf.Log[len(rf.Log)-1].Term)

				rf.mu.Lock()
				if rf.StatusType != candidate_type { //有可能发送完不是candidate了,不加的话可能会导致同个任期出现多个leader
					rf.mu.Unlock()
					return
				}
				if rf.CurrentTerm != current_term {
					rf.mu.Unlock()
					return
				}
				rf.mu.Unlock()

				rf.mu.Lock()
				//收到reply的任期较大，那我也得变为follower
				//all servers - 2
				if reply.Term > rf.CurrentTerm {
					rf.CurrentTerm = reply.Term
					rf.changeto(follower_type)
					rf.persist()
					rf.mu.Unlock()
					return
				}
				if reply.VoteGranted {
					countLock.Lock()
					count++
					countLock.Unlock()
					// DPrintfR("我的id：%v,我的任期:%v，我的身份:%v，收到几张投票：%v\n", rf.me, rf.CurrentTerm, rf.StatusType, count)
					if count > servers/2 { //获得超过一半的投票,变为leader
						rf.changeto(leader_type)
						rf.mu.Unlock()
						return
					}
				}
				rf.mu.Unlock()
			}(i)
		}
	}

	//保护goroutine的生命周期
	wg.Wait() // 等待所有 goroutine 完成
}

func (rf *Raft) changeto(changetype int) {
	// fmt.Printf("change:-------------id：%v ，变化：%v %v \n",rf.me,rf.StatusType,changetype)
	if changetype == candidate_type {
		rf.CurrentTerm++
		rf.VotedFor = len(rf.peers)
		rf.StatusType = candidate_type
	} else if changetype == leader_type {
		// rf.VotedFor = len(rf.peers)
		rf.StatusType = leader_type
		for i := 0; i < len(rf.peers); i++ {
			rf.NextIndex[i] = rf.Log[len(rf.Log)-1].Index + 1
			rf.MatchIndxt[i] = 0
		}
	} else {
		//如果不为follower才将投票信息清空
		// if rf.StatusType != follower_type {
		// 	rf.VotedFor = len(rf.peers)
		// }
		rf.VotedFor = len(rf.peers)
		// rf.reseteletiontime()
		rf.StatusType = follower_type
	}
}

// 更新重置超时时间
func (rf *Raft) reseteletiontime() {
	randtime := rand.Intn(300) + 300
	rf.ElectionTime = time.Now().Add(time.Duration(randtime) * time.Millisecond)
}
