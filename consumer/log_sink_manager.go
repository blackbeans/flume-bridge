package consumer

import (
	"flume-log-sdk/config"
	"flume-log-sdk/consumer/client"
	"github.com/blackbeans/redigo/redis"
	"log"
	"strconv"
	"sync"
	"time"
)

type SinkManager struct {
	zkmanager *config.ZKManager

	sinkServers map[string]*SinkServer //业务名称和sinkserver对应

	hp2flumeClientPool map[string]*flumeClientPool //对应的Pool

	mutex sync.Mutex
}

func NewSinkManager(option *config.Option) *SinkManager {

	sinkmanager := &SinkManager{}
	sinkmanager.sinkServers = make(map[string]*SinkServer)

	redisPool := initRedisQueue(option)
	//从zk中拉取flumenode的配置
	zkmanager := config.NewZKManager(option.Zkhost)
	sinkmanager.zkmanager = zkmanager

	initSinkServers(option.Businesses, zkmanager, func(business string, pools []*flumeClientPool) {
		//创建一个sinkserver
		sinkserver := newSinkServer(redisPool, pools)
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

func initSinkServers(businesses []string, zkmanager *config.ZKManager, callback func(business string, pools []*flumeClientPool)) {

	//开始创建一下对应的flume的agent的pool
	flumeMapping := make(map[string]*config.FlumeNode)
	for _, business := range businesses {
		flumeNode := zkmanager.GetAndWatch(business,
			func(path string, eventType config.ZkEvent) {
				//当前节点有发生变更,只关注删除该节点就行
				if eventType == config.Deleted {

				}
			},
			func(path string, childNode []config.HostPort) {
				//当前业务下的flume节点发生了变更会全量推送一次新的节点

			})
		flumeMapping[business] = flumeNode
	}

	hp2FlumePool := make(map[config.HostPort]*flumeClientPool)
	//创建flume的client
	for business, flumenodes := range flumeMapping {
		//开始构建对应关系
		pools := make([]*flumeClientPool, 0)
		for _, hp := range flumenodes.Flume {
			var pool *flumeClientPool
			existPool, ok := hp2FlumePool[hp]
			if ok {
				pool = existPool
			} else {
				pool = newFlumeClientPool(20, 50, 100, 10*time.Second, func() *client.FlumeClient {
					flumeclient := client.NewFlumeClient(hp.Host, hp.Port)
					flumeclient.Connect()
					return flumeclient
				})
				go monitorPool(hp.Host+":"+strconv.Itoa(hp.Port), pool)
			}
			pools = append(pools, pool)
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
