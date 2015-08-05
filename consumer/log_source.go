package consumer

import (
	"flume-bridge/consumer/pool"
	"flume-bridge/rpc/flume"
	"github.com/momotech/GoRedis/libs/stdlog"
	"math/rand"
	"net"
	"strings"
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
	clientPools  []*pool.FlumePoolLink
	isStop       bool
	monitorCount counter
	business     string
	batchSize    int
	buffChannel  chan *flume.ThriftFlumeEvent
	sourceLog    stdlog.Logger
	sendWorkNum  chan byte //发送线程数
}

var LOCAL_IP = []string{}

func init() {
	interfaces, err := net.Interfaces()
	if nil == err {
		for _, inter := range interfaces {
			if inter.Name == "bond0" {
				//如果是当前网卡则选择
				addrs, _ := inter.Addrs()
				for _, addr := range addrs {
					if strings.Index(addr.String(), "10.83") == 0 ||
						strings.Index(addr.String(), "172.30") == 0 {
						ip := strings.Split(addr.String(), "/")[0]
						LOCAL_IP = append(LOCAL_IP, ip)
					}
				}
				break
			}
		}
	}
}

func newSourceServer(business string, clientPools []*pool.FlumePoolLink, sourceLog stdlog.Logger) (server *SourceServer) {
	buffChannel := make(chan *flume.ThriftFlumeEvent)
	sourceServer := &SourceServer{
		business:    business,
		clientPools: clientPools,
		batchSize:   batchSize,
		buffChannel: buffChannel,
		sourceLog:   sourceLog}

	sendWorkNum := make(chan byte, 10)
	for i := 0; i < 10; i++ {
		sendWorkNum <- 1
	}

	sourceServer.sendWorkNum = sendWorkNum

	return sourceServer
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
	arrayPool = len(self.sendWorkNum)
	return
}

//启动pop
func (self *SourceServer) start() {

	self.isStop = false
	packets := make([]*flume.ThriftFlumeEvent, 0, self.batchSize)
	tick := time.NewTicker(1 * time.Second)
	for !self.isStop {
		select {
		case event := <-self.buffChannel:
			if len(packets) < self.batchSize {
				packets = append(packets, event)
			} else {
				<-self.sendWorkNum
				go func(p []*flume.ThriftFlumeEvent) {
					defer func() {
						self.sendWorkNum <- 1
					}()
					self.flush(p)
				}(packets)
				packets = packets[0:]
			}
		case <-tick.C:
			//超时如果有数据则直接flush
			if len(packets) > 0 {
				<-self.sendWorkNum
				go func(p []*flume.ThriftFlumeEvent) {
					defer func() {
						self.sendWorkNum <- 1
					}()
					self.flush(p)
				}(packets[0:len(packets)])
				packets = packets[0:]
			}
		}
	}
	self.sourceLog.Printf("LOG_SOURCE|SOURCE SERVER [%s]|STARTED\n", self.business)
}

func (self *SourceServer) flush(events []*flume.ThriftFlumeEvent) {

	for i := 0; i < 3; i++ {

		pool := self.getFlumeClientPool()
		if nil == pool {
			self.sourceLog.Printf("LOG_SOURCE|GET FLUMECLIENTPOOL|FAIL|%s|TRY:%d\n", self.business, i)
			continue
		}
		flumeclient, err := pool.Get(5 * time.Second)
		if nil != err || nil == flumeclient {
			self.sourceLog.Printf("LOG_SOURCE|GET FLUMECLIENT|FAIL|%s|%s|TRY:%d\n", self.business, err, i)
			continue
		}

		err = flumeclient.AppendBatch(events)
		defer func() {
			if err := recover(); nil != err {
				//回收这个坏的连接
				pool.ReleaseBroken(flumeclient)
			} else {
				pool.Release(flumeclient)
			}
		}()

		if nil != err {
			atomic.AddInt64(&self.monitorCount.currFailValue, int64(len(events)))
			self.sourceLog.Printf("LOG_SOURCE|SEND FLUME|FAIL|%s|%s|TRY:%d\n", self.business, err.Error(), i)

		} else {
			atomic.AddInt64(&self.monitorCount.currSuccValue, int64(1*self.batchSize))
			if rand.Int()%10000 == 0 {
				self.sourceLog.Printf("trace|send 2 flume succ|%s|%d\n", flumeclient.HostPort(), len(events))
			}
			break
		}

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

	local := make([]*pool.FlumeClientPool, 0, 5)
	remote := make([]*pool.FlumeClientPool, 0, 5)

	//首先筛选当前本地flume agent机器IP
	for _, cp := range self.clientPools {
		for _, ip := range LOCAL_IP {
			//如果是本机的则加入
			if ip == cp.FlumePool.GetHostPort().Host {
				local = append(local, cp.FlumePool)
			} else {
				remote = append(remote, cp.FlumePool)
			}
		}
	}

	//优先通过本地的flume-agent传输
	if len(local) > 0 {
		idx := rand.Intn(len(local))
		return local[idx]
	} else if len(remote) > 0 {
		//再通过远端的方式传送数据
		idx := rand.Intn(len(remote))
		return remote[idx]
	}

	return nil
}
