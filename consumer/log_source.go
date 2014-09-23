package consumer

import (
	"container/list"
	"encoding/json"
	"flume-log-sdk/config"
	"flume-log-sdk/consumer/client"
	"flume-log-sdk/consumer/pool"
	"flume-log-sdk/rpc/flume"
	"fmt"
	"github.com/momotech/GoRedis/libs/stdlog"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

type counter struct {
	lastSuccValue int64

	currSuccValue int64

	lastFailValue int64

	currFailValue int64
}

//生成对象池用于缓存 thriftEvent对象
var objpool *sync.Pool

func init() {
	objpool = &sync.Pool{}
	objpool.New = func() interface{} {
		//创建生成thriftevent的
		return client.NewFlumeEvent()
	}
}

// 用于向flume中作为sink 通过thrift客户端写入日志
type SourceServer struct {
	flumeClientPool *list.List
	isStop          bool
	monitorCount    counter
	business        string
	batchSize       int
	buffChannel     chan *flume.ThriftFlumeEvent
	sourceLog       stdlog.Logger
}

func newSourceServer(business string, flumePool *list.List, sourceLog stdlog.Logger) (server *SourceServer) {
	batchSize := 2000
	sendbuff := 100000
	buffChannel := make(chan *flume.ThriftFlumeEvent, sendbuff)
	sourceServer := &SourceServer{
		business:        business,
		flumeClientPool: flumePool,
		batchSize:       batchSize,
		buffChannel:     buffChannel,
		sourceLog:       sourceLog}
	return sourceServer
}

func (self *SourceServer) monitor() (succ, fail int64, bufferSize int) {
	currSucc := self.monitorCount.currSuccValue
	currFail := self.monitorCount.currFailValue
	succ = (currSucc - self.monitorCount.lastSuccValue)
	fail = (currFail - self.monitorCount.lastFailValue)
	self.monitorCount.lastSuccValue = currSucc
	self.monitorCount.lastFailValue = currFail

	//自己的Buffer大小
	bufferSize = len(self.buffChannel)
	return
}

//启动pop
func (self *SourceServer) start() {

	self.isStop = false

	//创建chan ,buffer 为10
	sendbuff := make(chan []*flume.ThriftFlumeEvent, self.batchSize)
	//启动20个go程从channel获取
	for i := 0; i < 10; i++ {
		go func(ch chan []*flume.ThriftFlumeEvent) {
			for !self.isStop {
				events := <-ch
				self.innerSend(events)
			}
		}(sendbuff)
	}

	go func() {
		//批量收集数据
		pack := make([]*flume.ThriftFlumeEvent, 0, self.batchSize)
		for !self.isStop {
			event := <-self.buffChannel
			//如果总数大于batchsize则提交
			if len(pack) < self.batchSize {
				//批量提交
				pack = append(pack, event)
				continue
			}
			sendbuff <- pack[:len(pack)]
			// pack = make([]*flume.ThriftFlumeEvent, 0, self.batchSize)
		}

		close(sendbuff)
	}()

	self.sourceLog.Printf("LOG_SOURCE|SOURCE SERVER [%s]|STARTED\n", self.business)
}

func (self *SourceServer) innerSend(events []*flume.ThriftFlumeEvent) {

	for i := 0; i < 3; i++ {

		pool := self.getFlumeClientPool()
		if nil == pool {
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

	//归还event对线到池子中
	defer func() {
		for _, v := range events {
			objpool.Put(*v)
		}
	}()
}

//解析出decodecommand
func decodeCommand(resp []byte) (string, *flume.ThriftFlumeEvent) {
	var cmd config.Command
	err := json.Unmarshal(resp, &cmd)
	if nil != err {
		log.Printf("command unmarshal fail ! %T | error:%s\n", resp, err.Error())
		return "", nil
	}
	//
	momoid := cmd.Params["momoid"].(string)

	businessName := cmd.Params["businessName"].(string)

	action := cmd.Params["type"].(string)

	bodyContent := cmd.Params["body"]

	//将businessName 加入到body中
	bodyMap := bodyContent.(map[string]interface{})
	bodyMap["business_type"] = businessName

	body, err := json.Marshal(bodyContent)
	if nil != err {
		log.Printf("marshal log body fail %s", err.Error())
		return businessName, nil
	}

	//拼Body
	flumeBody := fmt.Sprintf("%s\t%s\t%s", momoid, action, string(body))
	obj := objpool.Get()
	event := client.EventFillUp(obj, businessName, action, []byte(flumeBody))
	// event := client.NewFlumeEvent(businessName, action, []byte(flumeBody))
	return businessName, event
}

func (self *SourceServer) stop() {
	self.isStop = true
	time.Sleep(5 * time.Second)

	//遍历所有的flumeclientlink，将当前Business从该链表中移除
	for v := self.flumeClientPool.Back(); nil != v; v = v.Prev() {
		v.Value.(*pool.FlumePoolLink).DetachBusiness(self.business)
	}
	close(self.buffChannel)
	self.sourceLog.Printf("LOG_SOURCE|SOURCE SERVER|[%s]|STOPPED\n", self.business)
}

func (self *SourceServer) getFlumeClientPool() *pool.FlumeClientPool {

	//采用轮训算法
	e := self.flumeClientPool.Back()
	if nil == e {
		return nil
	}
	self.flumeClientPool.MoveToFront(e)
	return e.Value.(*pool.FlumePoolLink).FlumePool

}
