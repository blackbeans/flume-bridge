package consumer

import (
	"flume-bridge/config"
	"flume-bridge/consumer/pool"
	"github.com/garyburd/redigo/redis"
	"github.com/momotech/GoRedis/libs/stdlog"
	"log"
	"os"
	"strconv"
	"sync"
	"time"
)

type poolwrapper struct {
	hostport config.HostPort

	rpool *redis.Pool

	lastValue int64

	currValue int64
}

type SourceManager struct {
	zkmanager *config.ZKManager

	sourceServers map[string]*SourceServer //业务名称和sourceserver对应

	hp2flumeClientPool map[config.HostPort]*pool.FlumePoolLink //对应的Pool

	redispool map[string][]*poolwrapper // 对应的redispool

	watcherPool map[string]*config.Watcher //watcherPool

	mutex sync.Mutex

	isRunning bool

	instancename string

	flumeLog         stdlog.Logger
	redisLog         stdlog.Logger
	watcherLog       stdlog.Logger
	flumePoolLog     stdlog.Logger
	flumeSourceLog   stdlog.Logger
	sourceManagerLog stdlog.Logger
	option           *config.Option
}

func NewSourceManager(instancename string, option *config.Option) *SourceManager {

	sourcemanager := &SourceManager{}
	sourcemanager.option = option
	sourcemanager.sourceServers = make(map[string]*SourceServer)
	sourcemanager.hp2flumeClientPool = make(map[config.HostPort]*pool.FlumePoolLink)
	sourcemanager.watcherPool = make(map[string]*config.Watcher)

	//创建使用的Logger
	basepath := option.LogPath + "/" + instancename
	sourcemanager.sourceManagerLog = buildLog(basepath, "source_manager", "source_manager.log")
	sourcemanager.flumeLog = buildLog(basepath, "flume_tps", "flume_tps.log")
	sourcemanager.flumePoolLog = buildLog(basepath, "flume_pool", "flume_pool.log")
	sourcemanager.redisLog = buildLog(basepath, "redis_tps", "redis_tps.log")
	sourcemanager.watcherLog = buildLog(basepath, "zk_watcher", "zk_watcher.log")
	sourcemanager.flumeSourceLog = buildLog(basepath, "flume_source", "flume_source.log")

	sourcemanager.redispool = initRedisQueue(option)
	//从zk中拉取flumenode的配置
	zkmanager := config.NewZKManager(option.Zkhost)
	sourcemanager.zkmanager = zkmanager
	sourcemanager.instancename = instancename

	sourcemanager.initSourceServers(option.Businesses, zkmanager)
	sourcemanager.sourceManagerLog.Println("SOURCE_MANGER|Init SourceManager|Succ...")
	return sourcemanager

}

func buildLog(basepath, logname, filename string) stdlog.Logger {

	_, err := os.Stat(basepath)
	if nil != err {
		err := os.MkdirAll(basepath, os.ModePerm)
		if nil != err {
			panic(err)
		}
	}

	//创建redis的log
	f, err := os.OpenFile(basepath+"/"+filename, os.O_CREATE|os.O_WRONLY|os.O_APPEND, os.ModePerm)
	if nil != err {
		panic(err)
	}
	logger := stdlog.Log(logname)
	logger.SetOutput(f)
	logger.SetPrefix(func() string {
		now := time.Now()
		nt := now.Format("2006-01-02 15:04:05")
		return nt + "\t"
	})
	return logger
}

func initRedisQueue(option *config.Option) map[string][]*poolwrapper {
	redispool := make(map[string][]*poolwrapper, 0)

	//创建redis的消费连接
	for _, v := range option.QueueHostPorts {

		hp := v
		pool := redis.NewPool(func() (conn redis.Conn, err error) {
			conn, err = redis.DialTimeout("tcp", hp.Host+":"+strconv.Itoa(hp.Port),
				time.Duration(hp.Timeout)*time.Second,
				time.Duration(hp.Timeout)*time.Second,
				time.Duration(hp.Timeout)*time.Second)

			return
		}, hp.Maxconn/2)

		pools, ok := redispool[v.QueueName]
		if !ok {
			pools = make([]*poolwrapper, 0)
			redispool[v.QueueName] = pools
		}

		poolw := &poolwrapper{}
		poolw.hostport = v.HostPort
		poolw.rpool = pool
		redispool[v.QueueName] = append(pools, poolw)
	}

	return redispool
}

func (self *SourceManager) initSourceServers(businesses []string, zkmanager *config.ZKManager) {

	for _, business := range businesses {
		nodewatcher := newFlumeWatcher(business, self)
		flumeNode := zkmanager.GetAndWatch(business, nodewatcher)
		self.watcherPool[business] = nodewatcher
		sserver := self.initSourceServer(business, flumeNode)
		self.sourceServers[business] = sserver
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
		self.sourceManagerLog.Printf("unsupport business[%s],HostPorts:[%s]\n", business, flumenodes)
		return nil
	}

	if len(flumenodes) == 0 {
		self.sourceManagerLog.Println("no valid flume agent node for [" + business + "]")
		return nil
	}

	//新增的消费类型
	//使用的pool
	pools := self.initFlumeClientPool(business, flumenodes)

	//创建一个sourceserver
	sourceserver := newSourceServer(business, pools, self.flumeSourceLog)

	return sourceserver
}

func (self *SourceManager) initFlumeClientPool(business string, flumenodes []config.HostPort) []*pool.FlumePoolLink {

	pools := make([]*pool.FlumePoolLink, 0, 10)
	for _, hp := range flumenodes {
		poollink, ok := self.hp2flumeClientPool[hp]
		if !ok {
			err, tmppool := pool.NewFlumePoolLink(hp)
			if nil != err {
				self.sourceManagerLog.Println("SOURCE_MANGER|INIT FLUMEPOOLLINE|FAIL|%s", err)
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
				self.sourceManagerLog.Printf("SOURCE_MANGER|CREATE FLUMECLIENT|FAIL|[%s]\n", hp)
				poollink = nil
			}
		}()

		if nil == poollink {
			continue
		}

		poollink.AttachBusiness(business)
		pools = append(pools, poollink)
	}
	self.sourceManagerLog.Printf("SOURCE_MANGER|CREATE FLUMECLIENT|SUCCESS|[%s,%d]\n", business, len(flumenodes))

	return pools
}

func (self *SourceManager) Start() {

	self.sourceManagerLog.Println("SourceManager|Start|Begin...")
	for _, v := range self.sourceServers {
		go v.start()
	}
	self.isRunning = true
	go self.monitor()
	self.sourceManagerLog.Printf("LOG_SOURCE_MANGER|[%s]|STARTED\n", self.instancename)
	self.startWorker()
	self.sourceManagerLog.Println("SourceManager|Start|End...")

}

func (self *SourceManager) startWorker() {
	//k：queuename v:redis hostport
	for k, v := range self.redispool {
		self.sourceManagerLog.Println("LOG_SOURCE_MANGER|REDIS|[" + k + "]|START")
		for _, pool := range v {

			go func(queuename string, pool *poolwrapper) {
				//批量收集数据
				conn := pool.rpool.Get()
				defer conn.Close()
				self.sourceManagerLog.Printf("LOG_SOURCE_MANGER|REDIS-POP|BEGIN|%s|%s\n", queuename, pool.hostport)
				for self.isRunning {
					reply, err := conn.Do("LPOP", queuename)
					if nil != err || nil == reply {
						if nil != err {
							self.sourceManagerLog.Printf("LPOP|FAIL|%T", err)
							conn.Close()
							conn = pool.rpool.Get()
						} else {
							time.Sleep(100 * time.Millisecond)
						}
						continue
					}

					//计数器++
					pool.currValue++

					resp := reply.([]byte)

					if self.option.IsCompress {
						resp = decompress(resp)
					}
					if resp == nil {
						self.sourceManagerLog.Printf("LOG_SOURCE_MANGER|decompress|%s\n", resp)
						continue
					}
					businessName, logType, event := decodeCommand(resp)
					if nil == event {
						self.sourceManagerLog.Printf("LOG_SOURCE_MANGER|decodeCommand|%s\n", event)
						continue
					}

					//提交到对应business的channel中
					routeKey := businessName + logType
					defaultRoutKey := "default" + logType
					sourceServer, ok := self.sourceServers[routeKey]
					if !ok {
						//use the default channel
						sourceServer, ok := self.sourceServers[defaultRoutKey]
						if ok && nil != sourceServer && !sourceServer.isStop {
							sourceServer.buffChannel <- event
						} else {
							self.sourceManagerLog.Printf("LOG_SOURCE_MANGER|DEFAULT SOURCE_SERVER NOT EXSIT OR STOPPED\n")
						}
					} else {
						if !sourceServer.isStop {
							sourceServer.buffChannel <- event
						} else {
							self.sourceManagerLog.Printf("LOG_SOURCE_MANGER|SOURCE_SERVER STOPPED|%s\n", routeKey)
						}
					}
				}
				self.sourceManagerLog.Printf("LOG_SOURCE_MANGER|REDIS-POP|EXIT|%s|%s\n", queuename, pool.hostport)
			}(k, pool)

		}
	}

}

func (self *SourceManager) Close() {
	self.isRunning = false

	for _, sourceserver := range self.sourceServers {
		sourceserver.stop()
	}

	for _, redispool := range self.redispool {
		for _, pool := range redispool {
			pool.rpool.Close()
		}
	}

	//关闭flumepool
	for _, flumepool := range self.hp2flumeClientPool {
		flumepool.FlumePool.Destroy()
	}

	log.Printf("LOG_SOURCE_MANGER|[%s]|STOP\n", self.instancename)
}
