package kvraft

import (
	"crypto/rand"
	"math/big"
	"time"

	"6.824/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	id        int //客户端标识
	commandid int //最后发送的一条command
	leaderid  int //当前认为的leader
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

// 创建客户端
// 第i个端口connect第i个kv服务器
func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.id = int(nrand())
	ck.commandid = 1
	ck.leaderid = 0
	ck.servers = servers
	// You'll have to add code here.
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
// 获取一个该键的当前值，如果不存在则返回"",在面对其他所有错误时会持续尝试直到成功
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
// args 和 reply 的类型（包括它们是否为指针）必须与RPC处理函数的参数声明类型匹配。
// 并且 reply 必须以指针形式传递。
func (ck *Clerk) Get(key string) string {
	args := GetArgs{}
	args.Key = key
	args.Clerkid = ck.id
	args.Commandid = ck.commandid
	reply := GetReply{}
	for {
		// fmt.Printf("我是%v IN,我向%v发送:%v\n", ck.id%100, ck.leaderid,ck.commandid)
		ok := ck.servers[ck.leaderid].Call("KVServer.Get", &args, &reply)
		if ok && reply.Err == OK { //接受成功
			ck.commandid++
			return reply.Value
		} else {
			if ok && reply.Err == ErrNoKey {
				ck.commandid++
				return ""
			}
			if !ok || reply.Err == ErrWrongLeader || reply.Err == ErrTimeOut { //不是leader
				ck.leaderid = (ck.leaderid + 1) % len(ck.servers)
			}
			time.Sleep(20 * time.Microsecond)
		}
	}
	// You will have to modify this function.
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
// args 和 reply 的类型（包括它们是否为指针）必须与RPC处理函数的声明类型匹配。
// 并且 reply 必须以指针形式传递。
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	args := PutAppendArgs{}
	args.Key = key
	args.Value = value
	args.Op = op
	args.Clerkid = ck.id
	args.Commandid = ck.commandid
	reply := PutAppendReply{}
	for {
		// fmt.Printf("我是%v IN,我向%v发送:%v\n", ck.id%100, ck.leaderid,ck.commandid)
		ok := ck.servers[ck.leaderid].Call("KVServer.PutAppend", &args, &reply)
		// fmt.Printf("我是%v OUT,我向%v发送,ok?:%v\n", ck.id%100, ck.leaderid, ok)
		if ok && reply.Err == OK { //接受成功
			ck.commandid++
			return
		} else {
			if !ok || reply.Err == ErrWrongLeader || reply.Err == ErrTimeOut { //不是leader
				ck.leaderid = (ck.leaderid + 1) % len(ck.servers)
			}
			time.Sleep(20 * time.Microsecond)
		}
	}
	// You will have to modify this function.
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
