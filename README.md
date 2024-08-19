已完成lab2，lab3，实现了选举，日志复制，快照功能。实现了kvraft键值对数据库。

代码过程中出现的一些难点以及分析 

# 没开启协程来发送rpc

串行化发送rpc

发送者的身份：1 ，2向0发送了一次请求,发送任期：3，接受任期：0，结果为false

发送者的身份：1 ，2向1发送了一次请求,发送任期：3，接受任期：4，结果为false

我的id：2,我的任期:4，我的身份:0，收到几张投票：1 --------------- isbigterm：true

发送者的身份：1 ，1向0发送了一次请求,发送任期：3，接受任期：0，结果为false

发送者的身份：1 ，1向2发送了一次请求,发送任期：3，接受任期：4，结果为false

我的id：1,我的任期:4，我的身份:0，收到几张投票：1 --------------- isbigterm：true

我成为candidate了，我的id是：2，我的任期是：5，我的超时时间是：2024-05-14 09:50:39.444358555 +0800 CST m=+2.310792569

我成为candidate了，我的id是：1，我的任期是：5，我的超时时间是：2024-05-14 09:50:39.864340565 +0800 CST m=+2.730774584

发送者的身份：1 ，2向0发送了一次请求,发送任期：5，接受任期：0，结果为false

发送者的身份：1 ，2向1发送了一次请求,发送任期：5，接受任期：5，结果为false

我的id：2,我的任期:5，我的身份:1，收到几张投票：1 --------------- isbigterm：false

我成为candidate了，我的id是：2，我的任期是：6，我的超时时间是：2024-05-14 09:50:39.673782155 +0800 CST m=+2.540216129

发送者的身份：1 ，2向0发送了一次请求,发送任期：3，接受任期：0，结果为false

发送者的身份：1 ，2向1发送了一次请求,发送任期：3，接受任期：5，结果为false

即candidate协程进入后获得current_term，然后发送消息，而没开启协程发送rpc，rpc发送消息阻塞在那里，导致后来发送消息时，即使我的任期很大了，然而我赋值的current却是很久之前的term了 
通过innum堆积发现问题，并调试。

# lab2B实现选举操作的结构出现问题

在调试时总是出现同时发起选举。经过调试与问题排除，最终定位到问题在于论文理解错误。

ticker是用来检测选举计时器是否超时的，由于论文误解了论文中的意思，我的ticker实现的是循环进行选举操作，这样就导致了我的candidate可能在同个任期下发起多次选举，而论文中的意思是只有在选举计时器超时的时候才会发起选举。这也

间接导致了我后面出现了inum堆积问题，即发起选举的次数过于频繁了。由于我设置的是ticker是50ms，而超时时间设置的是500ms左右，但我原本那样写基本等价于50ms就发起一次选举。

而我一开始在调试代码时却偏向于去查找发起投票请求代码方面的错误，在将rpc发送请求修改为开启协程发送后，基本上解决了inum堆积问题后，跑测试脚本时还是会在100内出现几次错误。

经过两天调试也发现结果不理想。又去扒了几篇做这个实验的坑点的问题后，并再次阅读了助教实验指导书，才发现是一开始将发起投票选举理解错了。在修改了这一逻辑后，终于通过多次测试了。

# lab2C出现的棘手的apply error问题

出现apply error问题在于提交的条目不一致。在测试多次并查找日志错误后定位到问题。

我阅读了论文后，发现图二将心跳发送和发送条目同时作为appendrpc发送，所以我实现的代码中将这两个给写在一起了，当发送的条目为空时就将它认为是心跳，

而只有有条目时follower才会更新自己的日志，在网络不会特别糟糕的情况下不会出现问题，但到了图8unreliable的测试中，却会出现apply error的问题，我也上网搜索过一些人遇见这个报错的解决方案，

但没有找到能解决这个问题的。由于测试中的条目数量为上千条，一般情况下每次发送条目后输出每个服务器的日志，就已经需要输出7，8千行，所以我只能跑几百组测试，然后从中找出一些一两千行的日志来进行debug。

在查看了4，5千行日志后，最终让我找到了问题所在：心跳发送过来，通过了follower的前面几次判断，并没有修改到follower的日志，却进入了更新comitindex的分支，此时就导致了follower提交了和leader不一致的条目。

我的解决方法是在接受到心跳时更新commitindex时，将commitindex与prelogindex取个最小，这样就不会提交不一致的条目了。

# lab2D出现的锁的问题

最初发现这个问题是在snapshot函数中使用了.lock和defer .unlock。但是却发送了死锁。程序一直卡在那里不动。于是变按着调用路径去查找其调用过程，发现在另一个文件config.go中它在applier中会进行snap操作通知服务器进行快照操作。

我的第一反应是某个协程占用了这个锁一直没有释放。我的第一想法是直接阅读10几个加锁和解锁的位置的代码，但是读完后发现并没有什么问题。我也上网搜索了很多相关内容，没发现出现这种情况的。导致我非常怀疑。既然阅读代码解决不了问

题。我只能往调试的方向走，一开始我是对每一次加锁和解锁进行print来查看哪里没有解锁，但是由于并发调试下输出的内容非常多。我发现仅仅使用print去打印相关日志非常难解决问题。所以我去查找了一些go相关的检测阻塞问题的工具，使用

过-race和pprof，但是这些工具输出的内容太多而且只能输出在哪里阻塞的问题。而程序原本就存在一定的阻塞。所以定位不到问题所在。在被折磨了好几个小时后，我在询问某次ai的时候找到了一个很好的解决办法。就是将mutex封装成一个

debugmutex，这样就不需要去动到其他的代码了。然后我结合问题所在，想到一个做法是开启一个协程每秒去读取这个锁被占有的时间，如果超过一定时间就把它当前的栈调用情况输出出来。最终终于找到问题所在。在applytomache中使用了lock

和defer .unlock，然后在里面又将msg丢到apply通道中，接着config.go调用了snapshot，snapshot又使用了.lock和.unlock，这样就相当于连续使用了两次lock操作，导致程序死锁。

下面是重新封装的能够定位死锁位置的锁 

```
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
```

# 在lab3并发数据量大下出现的问题

## applytomache函数问题

一开始我的applytomache函数是在ticker中定期发起，但是这其实是不对的，多个applytomache函数协程存在在lab3数据量大的时候就出现了抢占锁的问题，导致server层实际上接收到的信息出现不一致的情况。

后面将applytomache在make创建服务器的时候就开启即可保证有序性了，当没有新的commit消息时就sleep一段时间

## rf.nextindex>最后一个日志的index 

这在理论上不可能存在这个问题的。

下面是我在查找这个问题所在的心路历程

（1）第一步：

查找了所有修改了rf.nextindex的位置，并在下面添加panic来判断是否修改了比lastlogindex还要大的rf.nextindex。但是跑了几千组都发现没修改到。这时就非常没有眉目。后来仔细分析这种情况，这些位置都没发现错误。

（2）第二步：

猜测可能是snapshot时把我的日志吃掉了，但是打印多次日志后发现问题并不在这里，每次snapshot时都是正确的。

（3）第三步

再次仔细思考，既然和逻辑的实现问题没有关系，有没可能是在极端情况下的并发导致的问题呢。由于我前面在做完lab2后已经能稳定通过上千次测试了。但在进行lab3时，给到了更多的数据和更大的并发量，那这可能是非常极端情况下导致的问题

吗。所以我猜想问题可能来自于并发出现了问题了。于是我用-race再去跑lab2，定位到了问题，就是在发送rpc时不能加锁，而我在发送前persist了一次，导致在非常极端的情况下persist慢了一点点，而nextindex没有被及时更新。

（4）第四步：

做完第三步之后还是并没有解决问题。但是这时我才顿悟有没有可能问题是出现在log的问题上，顺着这条线找下去，我找到在我重启raft节点时，我先开启了ticker进行操作，接着再执行readpersist读取持久化信息。这样就导致了还没完全初始

化完日志时，就已经发送了日志。我把它放到后面再跑500次没出现错误。为了验证正确性我又跑了1000次，这时又出现了错误。看来是改了这里只减小了错误的发生率，仔细分析后，即使发生上面所说的这种情况，发送日志的前提是要先成为

leader，成为leader的初始化就会更改nextindex。所以并不能解决这一问题。

（5）第五步：

看了太多日志，脑子有点晕，跑去运动了一下。晚上回来后重新整理了思路。这次脑子对思路比较清晰，一口气看了几千条日志，最终终于找到了错误所在。问题出现在snapshot上面，当由于这个lab2D的图中并没有对这一问题进行讨论。就是发送

者发过来的snapshot夹带着它的任期，而当他的任期比我高时，我应该转换为follwer。在我跑500次出现一两次错误的时，场景是这样的：一个少数节点的分区leader 重新加入到多数节点的分区中。此时存在两个leader，而多数节点的分区在发送

心跳前先发送了snapshot，导致了少数分区的leader接收到这个snapshot后进行快照，而在它推出分区前，它对多数分区中的某个服务器的nextserver比较大，而它没有变成follower，所以他选择发送日志，此时就出现了nextindex比它日志最后一

条要大的情况了。 
