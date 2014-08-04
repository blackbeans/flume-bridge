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

	redisPool map[string][]*redis.Pool // 对应的redispool

	watcherPool map[string]*config.Watcher //watcherPool

	mutex sync.Mutex
}

func NewSinkManager(option *config.Option) *SinkManager {

	sinkmanager := &SinkManager{}
	sinkmanager.sinkServers = make(map[string]*SinkServer)
	sinkmanager.hp2flumeClientPool = make(map[string]*FlumePoolLink)
	sinkmanager.watcherPool = make(map[string]*config.Watcher)

	sinkmanager.redisPool = initRedisQueue(option)
	//从zk中拉取flumenode的配置
	zkmanager := config.NewZKManager(option.Zkhost)
	sinkmanager.zkmanager = zkmanager

	sinkmanager.initSinkServers(option.Businesses, zkmanager)

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

func (self *SinkManager) initSinkServers(businesses []string, zkmanager *config.ZKManager) {

	for _, business := range businesses {
		nodewatcher := newFlumeWatcher(business, self)
		flumeNode := zkmanager.GetAndWatch(business, nodewatcher)
		self.watcherPool[business] = nodewatcher
		self.initSinkServer(business, flumeNode)
	}
}

func (self *SinkManager) initSinkServer(business string, flumenodes []config.HostPort) {

	//首先判断当前是否该sink支持该种business
	_, ok := self.watcherPool[business]
	if !ok {
		log.Printf("unsupport business[%s],HostPorts:[%s]\n", business, flumenodes)
		return
	}

	//新增的消费类型
	//使用的pool
	pools := make([]*FlumePoolLink, 0)
	for _, hp := range flumenodes {
		key := hp.Host + ":" + strconv.Itoa(hp.Port)
		poollink, ok := self.hp2flumeClientPool[key]
		if !ok {
			poollink = newFlumePoolLink(hp)
			self.hp2flumeClientPool[key] = poollink
			go monitorPool(key, poollink.flumePool)
		}

		poollink.mutex.Lock()
		poollink.businessLink.PushFront(business)
		pools = append(pools, poollink)
		poollink.mutex.Unlock()
	}

	//创建一个sinkserver
	sinkserver := newSinkServer(business, self.redisPool, pools)
	self.sinkServers[business] = sinkserver

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
		log.Printf("sinkserver start [%s]", name)
	}
}

func (self *SinkManager) Close() {
	//TODO
	for name, sinkserver := range self.sinkServers {
		sinkserver.stop()
		log.Printf("sinkserver stop [%s]", name)
	}
}
