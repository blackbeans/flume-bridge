package consumer

import (
	"flume-log-sdk/config"

	"github.com/blackbeans/redigo/redis"
	"log"
	"strconv"
	"sync"
	"time"
)

type SinkManager struct {
	zkmanager *config.ZKManager

	sinkServers map[string]*SinkServer //业务名称和sinkserver对应

	hp2flumeClientPool map[string]*FlumePoolLink //对应的Pool

	mutex sync.Mutex
}

func NewSinkManager(option *config.Option) *SinkManager {

	sinkmanager := &SinkManager{}
	sinkmanager.sinkServers = make(map[string]*SinkServer)

	redisPool := initRedisQueue(option)
	//从zk中拉取flumenode的配置
	zkmanager := config.NewZKManager(option.Zkhost)
	sinkmanager.zkmanager = zkmanager

	sinkmanager.initSinkServers(option.Businesses, zkmanager, func(business string, pools []*FlumePoolLink) {
		//创建一个sinkserver
		sinkserver := newSinkServer(business, redisPool, pools)
		sinkmanager.sinkServers[business] = sinkserver
	})

	return sinkmanager

}

func initRedisQueue(option *config.Option) map[string][]*redis.Pool {
	redisPool := make(map[string][]*redis.Pool, 0)

	//创建redis的消费连接
	for _, v := range option.QueueHostPorts {

		pool := redis.NewPool(func() (conn redis.Conn, err error) {

			conn, err = redis.DialTimeout("tcp", v.Host+":"+strconv.Itoa(v.Port),
				time.Duration(v.Timeout)*time.Second,
				time.Duration(v.Timeout)*time.Second,
				time.Duration(v.Timeout)*time.Second)

			return
		}, time.Duration(v.Timeout*2)*time.Second, v.Maxconn/2, v.Maxconn)

		pools, ok := redisPool[v.QueueName]
		if !ok {
			pools = make([]*redis.Pool, 0)
			redisPool[v.QueueName] = pools
		}
		redisPool[v.QueueName] = append(pools, pool)
	}

	return redisPool
}

func (self *SinkManager) initSinkServers(businesses []string, zkmanager *config.ZKManager,
	callback func(business string, pools []*FlumePoolLink)) {

	//开始创建一下对应的flume的agent的pool
	flumeMapping := make(map[string]*config.FlumeNode)
	for _, business := range businesses {
		flumeNode := zkmanager.GetAndWatch(business,
			func(businsess string, eventType config.ZkEvent) {
				//当前节点有发生变更,只关注删除该节点就行
				if eventType == config.Deleted {
					self.mutex.Lock()
					defer self.mutex.Unlock()
					val, ok := self.sinkServers[businsess]
					if ok {
						//关闭这个业务消费
						val.stop()
						delete(self.sinkServers, business)
						log.Printf("business:[%s] deleted\n", business)
					}
				}
			},
			func(businsess string, childNode []config.HostPort) {
				//当前业务下的flume节点发生了变更会全量推送一次新的节点
				self.mutex.Lock()
				defer self.mutex.Unlock()
				val, ok := self.sinkServers[businsess]
				if ok {
					//已经存在那么就检查节点变更
					for _, hp := range childNode {
						key := hp.Host + ":" + strconv.Itoa(hp.Port)
						//先创建该业务节点：
						pool, ok := self.hp2flumeClientPool[key]
						//如果存在Pool直接使用
						if ok {
							contain := false
							//检查该业务已有是否已经该flumepool
							for e := pool.businessLink.Back(); nil != e; e = e.Prev() {
								if e.Value.(string) == business {
									contain = true
									break
								}
							}

							//如果不包含则创建该池子并加入该业务对应的flumeclientpoollink中
							if !contain {
								val.flumeClientPool = append(val.flumeClientPool, pool)
								log.Printf("business:[%s] add flume :[\n", business, pool)
							}
							//如果已经包含了，则啥事都不干

						} else {
							//如果不存在该flumepool，直接创建并且添加到该pool种
							poollink := newFlumePoolLink(hp)
							self.hp2flumeClientPool[key] = poollink
							val.flumeClientPool = append(val.flumeClientPool, poollink)
							poollink.businessLink.PushFront(business)
						}
					}

				} else {
					//新增的消费类型
					//使用的pool
					pools := make([]*FlumePoolLink, 0)
					for _, hp := range childNode {
						key := hp.Host + ":" + strconv.Itoa(hp.Port)
						poollink, ok := self.hp2flumeClientPool[key]
						if !ok {
							poollink = newFlumePoolLink(hp)
							self.hp2flumeClientPool[key] = poollink
						}

						poollink.mutex.Lock()
						poollink.businessLink.PushFront(business)
						pools = append(pools, poollink)
						poollink.mutex.Unlock()
					}
					//调用创建
					callback(business, pools)
				}

			})
		flumeMapping[business] = flumeNode
	}

	hp2FlumePool := make(map[config.HostPort]*FlumePoolLink)
	//创建flume的client
	for business, flumenodes := range flumeMapping {
		//开始构建对应关系
		pools := make([]*FlumePoolLink, 0)
		for _, hp := range flumenodes.Flume {
			var poollink *FlumePoolLink
			existPoolLink, ok := hp2FlumePool[hp]
			if ok {
				poollink = existPoolLink
			} else {
				poollink := newFlumePoolLink(hp)
				hp2FlumePool[hp] = poollink
				go monitorPool(hp.Host+":"+strconv.Itoa(hp.Port), poollink.flumePool)
			}
			poollink.mutex.Lock()
			//为该poollinke attach business
			poollink.businessLink.PushFront(business)

			poollink.mutex.Unlock()

			pools = append(pools, poollink)
		}

		//使用创建好的资源初始化sinkserver
		callback(business, pools)
	}
}

func monitorPool(hostport string, pool *flumeClientPool) {
	for {
		time.Sleep(1 * time.Second)

		log.Printf("flume:%s|active:%d,core:%d,max:%d",
			hostport, pool.ActivePoolSize(), pool.CorePoolSize(), pool.maxPoolSize)
	}

}

func (self *SinkManager) Start() {
	for name, v := range self.sinkServers {
		v.start()
		log.Printf("sinkserver stop [%s]", name)
	}
}

func (self *SinkManager) Close() {
	//TODO
	for name, sinkserver := range self.sinkServers {
		sinkserver.stop()
		log.Printf("sinkserver stop [%s]", name)
	}
}
