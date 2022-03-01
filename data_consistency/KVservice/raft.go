package KVservice

import (
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"net/rpc"
	"sync"
	"time"
)

//定义常量3
const raftCount = 3

//leader
type Leader struct {
	//任期
	Term int
	//领导编号
	LeaderId int
}

//创建存储leader的对象
//最初任期为0，-1代表没编号
var leader = Leader{0, -1}

//声明raft节点类型
type Raft struct {
	//锁
	mu sync.Mutex
	//节点编号
	me int
	//当前任期
	currentTerm int
	//为哪个节点投票
	votedFor int
	//当前节点状态
	//0 follower  1 candidate  2 leader
	state int
	//发送最后一条消息的时间
	lastMessageTime int64
	//当前节点的领导
	currentLeader int

	//消息通道
	message chan bool
	//选举通道
	electCh chan bool
	//心跳信号
	heartBeat chan bool
	//返回心跳信号
	hearbeatRe chan bool
	//超时时间
	timeout int
}

func Start_candidate() {
	//过程：创建三个节点，最初是follower状态
	//如果出现candidate状态的节点，则开始投票
	//产生leader

	//创建三个节点
	for i := 0; i < raftCount; i++ {
		//定义Make() 创建节点
		Make(i)
	}

	//对raft结构体实现rpc注册
	rpc.Register(new(Raft))
	rpc.HandleHTTP()
	err := http.ListenAndServe(":8080", nil)
	if err != nil {
		log.Fatal(err)
	}

	//防止选举没完成，main结束了
	for {
	}
}

//创建节点
func Make(me int) *Raft {
	rf := &Raft{}
	//编号
	rf.me = me
	//给0  1  2三个节点投票，给谁都不投
	rf.votedFor = -1
	//0 follower
	rf.state = 0
	rf.timeout = 0
	//最初没有领导
	rf.currentLeader = -1
	//设置任期
	rf.setTerm(0)
	//通道
	rf.electCh = make(chan bool)
	rf.message = make(chan bool)
	rf.heartBeat = make(chan bool)
	rf.hearbeatRe = make(chan bool)
	//随机种子
	rand.Seed(time.Now().UnixNano())

	//选举的逻辑实现
	go rf.election()
	//心跳检查
	go rf.sendLeaderHeartBeat()

	return rf
}

func (rf *Raft) setTerm(term int) {
	rf.currentTerm = term
}

//设置节点选举
func (rf *Raft) election() {
	//设置标签
	var result bool
	//循环投票
	for {
		timeout := randRange(150, 300)
		//设置每个节点最后一条消息的时间
		rf.lastMessageTime = millisecond()
		select {
		case <-time.After(time.Duration(timeout) * time.Millisecond):
			fmt.Println("当前节点状态为：", rf.state)
		}
		result = false
		//选leader，如果选出leader，停止循环，result设置为true
		for !result {
			//选择谁为leader
			result = rf.election_one_rand(&leader)
		}
	}
}

//产生随机值
func randRange(min, max int64) int64 {
	//用于心跳信号的时间等
	return rand.Int63n(max-min) + min
}

//获取当前时间的毫秒数
func millisecond() int64 {
	return time.Now().UnixNano() / int64(time.Millisecond)
}

//选leader
func (rf *Raft) election_one_rand(leader *Leader) bool {
	//超时时间
	var timeout int64
	timeout = 100
	//投票数量
	var vote int
	//用于是否开始心跳信号的方法
	var triggerHeartbeat bool
	//当前时间戳对应的毫秒
	last := millisecond()
	//定义返回值
	success := false

	//首先，要成为candidate状态
	rf.mu.Lock()
	rf.becomeCandidate()
	rf.mu.Unlock()

	//开始选
	fmt.Println("start electing leader")
	for {
		//遍历所有节点进行投票
		for i := 0; i < raftCount; i++ {
			//遍历到不是自己，则进行拉票
			if i != rf.me {
				//其他节点，拉票
				go func() {
					//其他节点没有领导
					if leader.LeaderId < 0 {
						//操作选举通道
						rf.electCh <- true
					}
				}()
			}
		}
		//设置投票数量
		vote = 0
		triggerHeartbeat = false
		//遍历所有节点进行选举
		for i := 0; i < raftCount; i++ {
			//计算投票数量
			select {
			case ok := <-rf.electCh:
				if ok {
					//投票数量+1
					vote++
					//返回值success
					//大于总票数的一半
					success = vote > raftCount/2
					//成领导的状态
					//如果票数大于一半，且未出发心跳信号
					if success && !triggerHeartbeat {
						//选举成功
						//发心跳信号
						triggerHeartbeat = true

						rf.mu.Lock()
						//真正的成为leader
						rf.becomeLeader()
						rf.mu.Unlock()

						//由leader向其他节点发送心跳信号
						//心跳信号的通道
						rf.heartBeat <- true
						fmt.Println(rf.me, "号节点成为了leader")
						fmt.Println("leader发送心跳信号")
					}
				}
			}
		}
		//间隔时间小于100毫秒左右
		//若不超时，且票数大于一半，且当前有领导
		if timeout+last < millisecond() || (vote >= raftCount/2 || rf.currentLeader > -1) {
			//结束循环
			break
		} else {
			//没有选出leader
			select {
			case <-time.After(time.Duration(10) * time.Millisecond):
			}
		}
	}
	return success
}

//修改节点为candidate状态
func (rf *Raft) becomeCandidate() {
	//将节点状态变为1
	rf.state = 1
	//节点任期加1
	rf.setTerm(rf.currentTerm + 1)
	//设置为哪个节点投票
	rf.votedFor = rf.me
	//当前没有领导
	rf.currentLeader = -1
}

func (rf *Raft) becomeLeader() {
	//节点状态变为2，代表leader
	rf.state = 2
	rf.currentLeader = rf.me
}

//设置发送心跳信号的方法
//只考虑leader没有挂的情况
func (rf *Raft) sendLeaderHeartBeat() {
	for {
		select {
		case <-rf.heartBeat:
			//给leader返回确认信号
			rf.sendAppendEntriesImpl()
		}
	}
}

//返回给leader的确认信号
func (rf *Raft) sendAppendEntriesImpl() {
	//判断当前是否是leader节点
	if rf.currentLeader == rf.me {
		//声明返回确认信号的节点个数
		var success_count = 0

		//设置返回确认信号的子节点
		for i := 0; i < raftCount; i++ {
			//若当前不是本节点
			if i != rf.me {
				go func() {
					//子节点有返回
					//rf.hearbeatRe <- true

					//rpc
					rp, err := rpc.DialHTTP("tcp", "127.0.0.1:8080")
					if err != nil {
						log.Fatal(err)
					}
					//接收服务端发来的消息
					var ok = false
					er := rp.Call("Raft.Communication", Param{"hello"}, &ok)
					if er != nil {
						log.Fatal(err)
					}
					if ok {
						//rpc通信的情况下，子节点有返回
						rf.hearbeatRe <- true
					}
				}()
			}
		}
		//计算返回确认信号的子节点，若子节点个数>raftCount/2，则校验成功
		for i := 0; i < raftCount; i++ {
			select {
			case ok := <-rf.hearbeatRe:
				if ok {
					//记录返回确认信号的子节点的个数
					success_count++
					if success_count > raftCount/2 {
						fmt.Println("投票选举成功，校验心跳信号成功")
						log.Fatal("程序结束")
					}
				}
			}
		}
	}
}

//通过RPC实现分布式调用
//RPC：访问内存

//分布式通信
type Param struct {
	Msg string
}

//等待客户端消息
func (r *Raft) Communication(p Param, a *bool) error {
	fmt.Println(p.Msg)
	*a = true
	return nil
}
