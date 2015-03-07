package pool

import (
	"container/list"
	"errors"
	"flume-log-sdk/config"
	"flume-log-sdk/consumer/client"
	"log"
	"sync"
	"time"
)

//flumeclient的pool Link
type FlumePoolLink struct {
	FlumePool *FlumeClientPool

	BusinessLink *list.List //使用这个clientpool的业务名称

	Mutex sync.RWMutex //保证在并发情况下能够对list的操作安全
}

func NewFlumePoolLink(hp config.HostPort) (error, *FlumePoolLink) {
	err, pool := newFlumeClientPool(hp, 10, 30, 50, 10*time.Second, func() (error, *client.FlumeClient) {
		flumeclient := client.NewFlumeClient(hp.Host, hp.Port)
		err := flumeclient.Connect()
		return err, flumeclient
	})
	//将此pool封装为Link
	return err, &FlumePoolLink{FlumePool: pool, BusinessLink: list.New()}
}

func (self *FlumePoolLink) IsAttached(business string) bool {
	self.Mutex.RLock()
	defer self.Mutex.RUnlock()
	return self.exists(business)

}

func (self *FlumePoolLink) exists(business string) bool {
	for e := self.BusinessLink.Back(); nil != e; e = e.Prev() {
		if e.Value.(string) == business {
			return true
		}
	}
	return false
}

//将该business从link加入
func (self *FlumePoolLink) AttachBusiness(business string) {
	self.Mutex.Lock()
	defer self.Mutex.Unlock()
	if !self.exists(business) {
		self.BusinessLink.PushFront(business)
	}
}

//将该business从link重移除
func (self *FlumePoolLink) DetachBusiness(business string) {
	self.Mutex.Lock()
	defer self.Mutex.Unlock()
	for e := self.BusinessLink.Back(); nil != e; e = e.Prev() {
		if e.Value.(string) == business {
			self.BusinessLink.Remove(e)
		}
	}

}

//flume连接池
type FlumeClientPool struct {
	dialFunc     func() (error, *client.FlumeClient)
	maxPoolSize  int //最大尺子大小
	minPoolSize  int //最小连接池大小
	corepoolSize int //核心池子大小

	poolSize int //当前正在运行的client

	idletime time.Duration //空闲时间

	idlePool *list.List //连接的队列

	checkOutPool *list.List //已经获取的poolsize

	mutex sync.RWMutex

	hostport config.HostPort

	running bool
}

type IdleClient struct {
	flumeclient *client.FlumeClient
	expiredTime time.Time
}

func newFlumeClientPool(hostport config.HostPort, minPoolSize, corepoolSize,
	maxPoolSize int, idletime time.Duration, dialFunc func() (error, *client.FlumeClient)) (error, *FlumeClientPool) {

	idlePool := list.New()
	checkOutPool := list.New()
	clientpool := &FlumeClientPool{
		hostport:     hostport,
		maxPoolSize:  maxPoolSize,
		corepoolSize: corepoolSize,
		minPoolSize:  minPoolSize,
		poolSize:     0,
		idletime:     idletime,
		idlePool:     idlePool,
		dialFunc:     dialFunc,
		checkOutPool: checkOutPool,
		running:      true}

	clientpool.mutex.Lock()
	defer clientpool.mutex.Unlock()
	//初始化一下最小的Poolsize,让入到idlepool中
	for i := 0; i < clientpool.minPoolSize; i++ {
		j := 0
		var err error
		var flumeclient *client.FlumeClient
		for ; j < 3; j++ {
			err, flumeclient = dialFunc()
			if nil != err {
				log.Printf("FLUME_POOL|INIT|FAIL|CREATE CLIENT|%s\n", err)

			} else {
				break
			}
		}

		if j >= 3 {
			return errors.New("FLUME_POOL|INIT|FAIL|" + err.Error()), nil
		}

		idleClient := &IdleClient{flumeclient: flumeclient, expiredTime: (time.Now().Add(clientpool.idletime))}
		clientpool.idlePool.PushFront(idleClient)
		clientpool.poolSize++
	}
	//启动链接过期
	go clientpool.evict()
	return nil, clientpool
}

func (self *FlumeClientPool) evict() {
	for self.running {

		select {
		case <-time.After(self.idletime):
			self.mutex.Lock()
			//池子中没有请求的时候做一下连接清理
			if self.checkOutPool.Len() <= 0 {
				continue
			}

			for e := self.idlePool.Back(); nil != e; e = e.Prev() {
				idleclient := e.Value.(*IdleClient)
				//如果当前时间在过期时间之后并且活动的链接大于corepoolsize则关闭
				isExpired := idleclient.expiredTime.Before(time.Now())
				if isExpired &&
					self.poolSize >= self.corepoolSize {
					idleclient.flumeclient.Destroy()
					idleclient = nil
					self.idlePool.Remove(e)
					//并且该表当前的active数量
					self.poolSize--
				} else if isExpired {
					//过期的但是已经不够corepoolsize了直接重新设置过期时间
					idleclient.expiredTime = time.Now().Add(self.idletime)
				} else {
					//活动的数量小于corepool的则修改存活时间
					idleclient.expiredTime = time.Now().Add(self.idletime)
				}
			}
			self.mutex.Unlock()
		}
	}
}

func (self *FlumeClientPool) GetHostPort() config.HostPort {
	return self.hostport
}

func (self *FlumeClientPool) MonitorPool() (int, int, int) {
	return self.checkOutPool.Len(), self.poolSize, self.maxPoolSize

}
func (self *FlumeClientPool) Get(timeout time.Duration) (*client.FlumeClient, error) {

	if !self.running {
		return nil, errors.New("flume pool has been stopped!")
	}

	//***如果在等待的时间内没有获取到client则超时
	var fclient *client.FlumeClient
	clientch := make(chan *client.FlumeClient, 1)
	defer close(clientch)
	go func() {
		fclient := self.innerGet()
		clientch <- fclient
	}()

	select {
	case fclient = <-clientch:
		return fclient, nil
	case <-time.After(timeout):
		return nil, errors.New("get client timeout!")

	}

}

//返回当前的poolszie
func (self *FlumeClientPool) PoolSize() int {
	self.mutex.RLock()
	defer self.mutex.RUnlock()
	return self.poolSize

}

func (self *FlumeClientPool) ActivePoolSize() int {
	self.mutex.RLock()
	defer self.mutex.RUnlock()
	return self.checkOutPool.Len()
}

//释放坏的资源
func (self *FlumeClientPool) ReleaseBroken(fclient *client.FlumeClient) error {
	self.mutex.Lock()
	defer self.mutex.Unlock()
	_, err := self.innerRelease(fclient)
	return err
}

func (self *FlumeClientPool) innerRelease(fclient *client.FlumeClient) (bool, error) {
	for e := self.checkOutPool.Back(); nil != e; e = e.Prev() {
		checkClient := e.Value.(*client.FlumeClient)
		if fclient == checkClient {
			self.checkOutPool.Remove(e)
			if fclient.IsAlive() {
				idleClient := &IdleClient{flumeclient: fclient, expiredTime: (time.Now().Add(self.idletime))}
				self.idlePool.PushFront(idleClient)
			} else {
				self.poolSize--
			}
			// log.Println("client return pool ")
			return true, nil
		}
	}

	//如果到这里，肯定是Bug，释放了一个游离态的客户端
	return false, errors.New("invalid flume client , this is not managed by pool")

}

/**
* 归还当前的连接
**/
func (self *FlumeClientPool) Release(fclient *client.FlumeClient) error {
	self.mutex.Lock()
	defer self.mutex.Unlock()
	//从checkoutpool中移除
	_, err := self.innerRelease(fclient)
	return err

}

//从现有队列中获取，没有了就创建、有就获取达到上限就阻塞
func (self *FlumeClientPool) innerGet() *client.FlumeClient {

	var fclient *client.FlumeClient
	self.mutex.RLock()
	//优先从空闲链接中获取链接
	for i := 0; i < self.idlePool.Len(); i++ {
		back := self.idlePool.Back()
		idle := back.Value.(*IdleClient)
		fclient = idle.flumeclient
		self.checkOutPool.PushFront(fclient)
		self.idlePool.Remove(back)
		break
	}
	self.mutex.RUnlock()

	//如果client还是没有那么久创建链接
	if nil == fclient {
		//工作连接数和空闲连接数已经达到最大的连接数上限
		if self.poolSize >= self.maxPoolSize {
			log.Printf("FlumeClientPool|innerGet|FULL|minPoolSize:%d,maxPoolSize:%d,poolSize:%d,activePoolSize:%d\n ",
				self.minPoolSize, self.maxPoolSize, self.checkOutPool.Len())
			return fclient
		} else {

			//如果没有可用链接则创建一个
			err, newClient := self.dialFunc()
			if nil != err {
				log.Printf("FlumeClientPool|innerGet|dialFunc|FAIL|%s\n", err)
			} else {
				self.mutex.Lock()
				defer self.mutex.Unlock()
				self.checkOutPool.PushFront(newClient)
				fclient = newClient
				self.poolSize++
			}

		}
	}
	return fclient
}

func (self *FlumeClientPool) Destroy() {
	self.mutex.Lock()
	self.running = false
	self.mutex.Unlock()
	for i := 0; i < 3; {
		time.Sleep(5 * time.Second)
		if self.ActivePoolSize() <= 0 {
			break
		}
		log.Printf("flume client pool closing : activepool:%d\n", self.ActivePoolSize())
		i++
	}

	self.mutex.Lock()
	defer self.mutex.Unlock()
	//关闭掉空闲的client
	for e := self.idlePool.Front(); e != nil; e = e.Next() {
		fclient := e.Value.(*IdleClient)
		fclient.flumeclient.Destroy()
		self.idlePool.Remove(e)
		fclient = nil
	}
	//关闭掉已经
	for e := self.checkOutPool.Front(); e != nil; e = e.Next() {
		fclient := e.Value.(*client.FlumeClient)
		fclient.Destroy()
		self.checkOutPool.Remove(e)
		fclient = nil
	}

	log.Printf("FLUME_POOL|DESTORY|%s", self.GetHostPort())
}
