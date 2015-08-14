package consumer

import (
	"flume-bridge/consumer/pool"
	"flume-bridge/rpc/flume"
	"github.com/momotech/GoRedis/libs/stdlog"
	"math/rand"
	"net"
	"os"
	"sync/atomic"
	"time"
)

type counter struct {
	lastSuccValue int64

	currSuccValue int64

	lastFailValue int64

	currFailValue int64
}

const (
	batchSize = 500
)

// 用于向flume中作为sink 通过thrift客户端写入日志
type SourceServer struct {
	clientPools       []*pool.FlumePoolLink
	clientPoolsRemote []*pool.FlumePoolLink
	clientPoolsLocal  []*pool.FlumePoolLink
	isStop            bool
	monitorCount      counter
	business          string
	batchSize         int
	buffChannel       chan *flume.ThriftFlumeEvent
	sourceLog         stdlog.Logger
	flushWorkNum      chan byte //发送线程数
	flushChan         chan []*flume.ThriftFlumeEvent
}

func newSourceServer(business string, clientPools []*pool.FlumePoolLink, sourceLog stdlog.Logger) (server *SourceServer) {
	buffChannel := make(chan *flume.ThriftFlumeEvent)
	sourceServer := &SourceServer{
		business:    business,
		clientPools: clientPools,
		batchSize:   batchSize,
		buffChannel: buffChannel,
		sourceLog:   sourceLog}

	sourceServer.spliteLocal()
	sourceServer.flushWorkNum = make(chan byte, 10)
	sourceServer.flushChan = make(chan []*flume.ThriftFlumeEvent, 1000)
	return sourceServer
}

//分离出本机和remote的flume pool
func (self *SourceServer) spliteLocal() {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		panic(err)
	}

	flumePoolRemote := make([]*pool.FlumePoolLink, 0)
	flumePoolLocal := make([]*pool.FlumePoolLink, 0)

	hostname, _ := os.Hostname()
	for _, pool := range self.clientPools {
		host := pool.FlumePool.GetHostPort().Host
		for _, addr := range addrs {
			self.sourceLog.Printf("[%s] [%s] [%s]\n", host, addr.String(), hostname)
			if host == addr.String() || host == hostname {
				flumePoolLocal = append(flumePoolLocal, pool)
				continue
			}
			flumePoolRemote = append(flumePoolRemote, pool)
		}
	}

	self.sourceLog.Printf("flume nodes|Remote :[%s] Local :[%s]\n", flumePoolRemote, flumePoolLocal)
	self.clientPoolsRemote = flumePoolRemote
	self.clientPoolsLocal = flumePoolLocal
}

func (self *SourceServer) monitor() (succ, fail int64, bufferSize, arrayPool int) {
	currSucc := self.monitorCount.currSuccValue
	currFail := self.monitorCount.currFailValue
	succ = (currSucc - self.monitorCount.lastSuccValue)
	fail = (currFail - self.monitorCount.lastFailValue)
	self.monitorCount.lastSuccValue = currSucc
	self.monitorCount.lastFailValue = currFail

	//自己的Buffer大小
	bufferSize = len(self.buffChannel)
	arrayPool = len(self.flushWorkNum)
	return
}

//启动pop
func (self *SourceServer) start() {

	self.isStop = false
	//消费
	go self.consume()
	//消息转移
	go self.transfer()
	self.sourceLog.Printf("LOG_SOURCE|SOURCE SERVER [%s]|STARTED\n", self.business)
}

func (self *SourceServer) consume() {

	//开启flush的操作
	go func() {
		for !self.isStop {
			events := <-self.flushChan
			self.flushWorkNum <- 1
			go func(p []*flume.ThriftFlumeEvent) {
				defer func() {
					<-self.flushWorkNum
				}()
				self.flush(p)
			}(events)
		}
	}()
}

//转移到消费的channel
func (self *SourceServer) transfer() {
	// transfer
	tick := time.NewTicker(500 * time.Millisecond)
	packets := make([]*flume.ThriftFlumeEvent, 0, self.batchSize)
	for !self.isStop {
		select {
		case event := <-self.buffChannel:
			if len(packets) < self.batchSize {
				packets = append(packets, event)
			} else {
				self.flushChan <- packets
				packets = make([]*flume.ThriftFlumeEvent, 0, self.batchSize)
			}
		case <-tick.C:
			//超时如果有数据则直接flush
			if len(packets) > 0 {
				self.flushChan <- packets[0:len(packets)]
				packets = make([]*flume.ThriftFlumeEvent, 0, self.batchSize)
			}
		}
	}

	//最后的flush出去
	if len(packets) > 0 {
		self.flushChan <- packets[0:len(packets)]
	}

}

func (self *SourceServer) flush(events []*flume.ThriftFlumeEvent) {

	start := time.Now().UnixNano()
	pool := self.getFlumeClientPool()
	if nil == pool {
		self.sourceLog.Printf("LOG_SOURCE|GET FLUMECLIENTPOOL|FAIL|%s\n", self.business)
		return
	}
	flumeclient, err := pool.Get(2 * time.Second)
	if nil != err || nil == flumeclient {
		self.sourceLog.Printf("LOG_SOURCE|GET FLUMECLIENT|FAIL|%s|%s\n",
			self.business, err.Error())
		return
	}

	defer func() {
		if err := recover(); nil != err {
			//回收这个坏的连接
			pool.ReleaseBroken(flumeclient)
		} else {
			pool.Release(flumeclient)
		}
	}()
	err = flumeclient.AppendBatch(events)

	if nil != err {
		atomic.AddInt64(&self.monitorCount.currFailValue, int64(len(events)))
		self.sourceLog.Printf("LOG_SOURCE|SEND FLUME|FAIL|%s|%s|%s\n",
			self.business, flumeclient.HostPort(), err.Error())
	} else {
		atomic.AddInt64(&self.monitorCount.currSuccValue, int64(1*self.batchSize))

	}

	end := time.Now().UnixNano()
	if rand.Intn(1000) == 0 {
		self.sourceLog.Printf("LOG_SOURCE|SEND FLUME|COST|%s|%s|cost:%d\n",
			self.business, flumeclient.HostPort(), end-start)
	}
}

func (self *SourceServer) stop() {
	self.isStop = true
	time.Sleep(5 * time.Second)

	//遍历所有的flumeclientlink，将当前Business从该链表中移除
	for _, v := range self.clientPools {
		v.DetachBusiness(self.business)
	}
	self.sourceLog.Printf("LOG_SOURCE|SOURCE SERVER|[%s]|STOPPED\n", self.business)
}

func (self *SourceServer) getFlumeClientPool() *pool.FlumeClientPool {
	//先从本地选择
	localCnt := len(self.clientPoolsLocal)
	if localCnt > 0 {
		idx := rand.Intn(localCnt)
		if idx < len(self.clientPoolsLocal) {
			return self.clientPoolsLocal[idx].FlumePool
		}
	}
	remoteCnt := len(self.clientPoolsRemote)
	if remoteCnt > 0 {
		idx := rand.Intn(remoteCnt)
		if idx < len(self.clientPoolsRemote) {
			return self.clientPoolsRemote[idx].FlumePool
		}
	}
	return nil
}
