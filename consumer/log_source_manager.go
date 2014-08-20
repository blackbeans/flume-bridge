package consumer

import (
	"container/list"
	"flume-log-sdk/config"
	"flume-log-sdk/consumer/pool"
	"fmt"
	"github.com/blackbeans/redigo/redis"
	"log"
	"os"
	"sort"
	"strconv"
	"sync"
	"time"
)

type SourceManager struct {
	zkmanager *config.ZKManager

	sourceServers map[string]*SourceServer //业务名称和sourceserver对应

	hp2flumeClientPool map[config.HostPort]*pool.FlumePoolLink //对应的Pool

	redisPool map[string][]*redis.Pool // 对应的redispool

	watcherPool map[string]*config.Watcher //watcherPool

	mutex sync.Mutex

	isRunning bool

	instancename string
}

func NewSourceManager(instancename string, option *config.Option) *SourceManager {

	sourcemanager := &SourceManager{}
	sourcemanager.sourceServers = make(map[string]*SourceServer)
	sourcemanager.hp2flumeClientPool = make(map[config.HostPort]*pool.FlumePoolLink)
	sourcemanager.watcherPool = make(map[string]*config.Watcher)
	sourcemanager.redisPool = initRedisQueue(option)
	//从zk中拉取flumenode的配置
	zkmanager := config.NewZKManager(option.Zkhost)
	sourcemanager.zkmanager = zkmanager
	sourcemanager.instancename = instancename

	sourcemanager.initSourceServers(option.Businesses, zkmanager)
	return sourcemanager

}

func (self *SourceManager) monitorFlume() {
	for self.isRunning {
		time.Sleep(1 * time.Second)
		monitor := "FLUME_TPS|"
		for k, v := range self.sourceServers {

			succ, fail := v.monitor()
			monitor += fmt.Sprintf("%s|%d/%d \t", k, succ, fail)
		}
		log.Println(monitor)

		mk := make([]string, 0)
		monitor = "FLUME_POOL|\n"
		for k, _ := range self.hp2flumeClientPool {
			mk = append(mk, k.Host+":"+strconv.Itoa(k.Port))
		}
		sort.Strings(mk)

		for _, hp := range mk {
			v, ok := self.hp2flumeClientPool[config.NewHostPort(hp)]
			if !ok {
				continue
			}
			active, core, max := v.FlumePool.MonitorPool()
			monitor += fmt.Sprintf("%s|%d/%d/%d\n", hp, active, core, max)
		}

		log.Println(monitor)
	}
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

func (self *SourceManager) initSourceServers(businesses []string, zkmanager *config.ZKManager) {

	for _, business := range businesses {
		nodewatcher := newFlumeWatcher(business, self)
		flumeNode := zkmanager.GetAndWatch(business, nodewatcher)
		self.watcherPool[business] = nodewatcher
		self.initSourceServer(business, flumeNode)
	}

	//-------------------注册当前进程ID到zk
	currpid := os.Getpid()
	hostname, _ := os.Hostname()
	self.zkmanager.RegistePath(businesses, hostname+"_"+self.instancename+":"+strconv.Itoa(currpid))

}

func (self *SourceManager) initSourceServer(business string, flumenodes []config.HostPort) *SourceServer {

	//首先判断当前是否该sink支持该种business
	_, ok := self.watcherPool[business]
	if !ok {
		log.Printf("unsupport business[%s],HostPorts:[%s]\n", business, flumenodes)
		return nil
	}

	if len(flumenodes) == 0 {
		log.Println("no valid flume agent node for [" + business + "]")
		return nil
	}

	//新增的消费类型
	//使用的pool
	pools := list.New()
	// pools := make([]*pool.FlumePoolLink, 0)
	for _, hp := range flumenodes {
		poollink, ok := self.hp2flumeClientPool[hp]
		if !ok {
			err, tmppool := pool.NewFlumePoolLink(hp)
			if nil != err {
				log.Println("SOURCE_MANGER|INIT FLUMEPOOLLINE|FAIL|%s", err)
				continue
			}
			poollink = tmppool
			self.hp2flumeClientPool[hp] = poollink
		}

		defer func() {
			if nil == poollink {
				return
			}
			if err := recover(); nil != err {
				log.Printf("create flumeclient fail :flume:[%s]\n", hp)
				poollink = nil
			}
		}()

		if nil == poollink {
			continue
		}

		poollink.AttachBusiness(business)
		pools.PushFront(poollink)
	}

	//创建一个sourceserver
	sourceserver := newSourceServer(business, self.redisPool, pools)
	self.sourceServers[business] = sourceserver
	return sourceserver

}

func (self *SourceManager) Start() {

	for _, v := range self.sourceServers {
		v.start()
	}
	self.isRunning = true
	go self.monitorFlume()
	log.Printf("LOG_SOURCE_MANGER|[%s]|STARTED\n", self.instancename)
}

func (self *SourceManager) Close() {
	for _, sourceserver := range self.sourceServers {
		sourceserver.stop()
	}

	for _, redispool := range self.redisPool {
		for _, pool := range redispool {
			pool.Close()
		}
	}

	//关闭flumepool
	for _, flumepool := range self.hp2flumeClientPool {
		flumepool.FlumePool.Destroy()
	}
	self.isRunning = false
	log.Printf("LOG_SOURCE_MANGER|[%s]|STOP\n", self.instancename)
}
