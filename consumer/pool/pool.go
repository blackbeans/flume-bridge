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

	Mutex sync.Mutex //保证在并发情况下能够对list的操作安全
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
	self.Mutex.Lock()
	defer self.Mutex.Unlock()
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

//将该business从link重移除
func (self *FlumePoolLink) AttachBusiness(business string) {
	self.Mutex.Lock()
	if self.exists(business) {
		self.BusinessLink.PushFront(business)
	}
	defer self.Mutex.Unlock()
}

//将该business从link重移除
func (self *FlumePoolLink) DetachBusiness(business string) {
	self.Mutex.Lock()
	for e := self.BusinessLink.Back(); nil != e; e = e.Prev() {
		if e.Value.(string) == business {
			self.BusinessLink.Remove(e)
		}
	}
	self.Mutex.Unlock()
}

//flume连接池
type FlumeClientPool struct {
	dialFunc     func() (error, *client.FlumeClient)
	maxPoolSize  int //最大尺子大小
	minPoolSize  int //最小连接池大小
	corepoolSize int //核心池子大小
	// activePoolSize int //当前正在运行的client

	idletime time.Duration //空闲时间

	idlePool *list.List //连接的队列

	checkOutPool *list.List //已经获取的poolsize

	mutex sync.Mutex

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
		// clientpool.activePoolSize++
	}
	return nil, clientpool
}

func (self *FlumeClientPool) GetHostPort() config.HostPort {
	return self.hostport
}

func (self *FlumeClientPool) MonitorPool() (int, int, int) {
	return self.ActivePoolSize(), self.CorePoolSize(), self.maxPoolSize

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

	ch := make(chan bool, 1)
	defer close(ch)
	select {
	case fclient = <-clientch:
		ch <- false
		break
	case <-time.After(time.Second * timeout):
		ch <- true
		break
	}

	isTimeout := <-ch
	//如果超时直接返回
	if isTimeout {
		return fclient, errors.New("get client timeout!")
	} else {
		return fclient, nil
	}
}

//返回当前的corepoolszie
func (self *FlumeClientPool) CorePoolSize() int {
	return self.idlePool.Len() + self.checkOutPool.Len()

}

func (self *FlumeClientPool) ActivePoolSize() int {
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

	idleClient := &IdleClient{flumeclient: fclient, expiredTime: (time.Now().Add(self.idletime))}
	self.mutex.Lock()
	defer self.mutex.Unlock()

	//从checkoutpool中移除
	succ, err := self.innerRelease(fclient)
	if nil != err {
		return err
	}

	//如果当前的corepoolsize 是大于等于设置的corepoolssize的则直接销毁这个client
	if self.CorePoolSize() >= self.corepoolSize {

		idleClient.flumeclient.Destroy()
		fclient = nil

		//并且从idle
	} else if succ {
		self.idlePool.PushFront(idleClient)
	} else {
		fclient.Destroy()
		fclient = nil
	}

	return nil

}

//从现有队列中获取，没有了就创建、有就获取达到上限就阻塞
func (self *FlumeClientPool) innerGet() *client.FlumeClient {

	var fclient *client.FlumeClient
	//首先检查一下当前空闲连接中是否有需要关闭的
	self.mutex.Lock()
	defer self.mutex.Unlock()
	for back := self.idlePool.Back(); back != nil; back = back.Prev() {
		// push ---> front ----> back 最旧的client
		idle := (back.Value).(*IdleClient)

		//如果已经挂掉直接移除
		if !idle.flumeclient.IsAlive() {
			self.idlePool.Remove(back)
			idle.flumeclient.Destroy()
			continue
		}

		//只有在corepoolsize>最小的池子大小，才去检查过期连接
		if self.CorePoolSize() > self.minPoolSize {
			//如果过期时间实在当前时间之后那么后面的都不过期
			if idle.expiredTime.After(time.Now()) {
				//判断一下当前连接的状态是否为alive 否则直接销毁
				self.idlePool.Remove(back)
				idle.flumeclient.Destroy()
			}
		} else {
			//如果小于等于Minpoolsize时，如果过期就将时间重置

			if idle.expiredTime.After(time.Now()) {
				idle.expiredTime = time.Now().Add(self.idletime)
			}
		}
	}

	//优先从空闲链接中获取链接
	for i := 0; i < self.idlePool.Len(); i++ {
		back := self.idlePool.Back()
		idle := back.Value.(*IdleClient)
		fclient = idle.flumeclient
		self.checkOutPool.PushFront(fclient)
		self.idlePool.Remove(back)
		break
	}

	//如果client还是没有那么久创建链接
	if nil == fclient {
		//工作连接数和空闲连接数已经达到最大的连接数上限
		if self.CorePoolSize() >= self.maxPoolSize {
			log.Printf("FLUME_POOL_FULL|minPoolSize:%d,maxPoolSize:%d,corePoolSize:%d,activePoolSize:%d\n ",
				self.minPoolSize, self.maxPoolSize, self.CorePoolSize(), self.ActivePoolSize())
			return fclient
		} else {
			//如果没有可用链接则创建一个
			err, newClient := self.dialFunc()
			if nil != err {

			} else {
				self.checkOutPool.PushFront(newClient)
				fclient = newClient
			}
		}
	}

	//检查是否corepool>= minpool,否则就创建连接
	if self.CorePoolSize() < self.minPoolSize {
		for i := self.CorePoolSize(); i <= self.minPoolSize; i++ {
			//如果没有可用链接则创建一个
			err, fclient := self.dialFunc()
			if nil != err {
				log.Printf("POOL|corepool(%d) < minpool(%d)|CREATE CLIENT FAIL|%s",
					self.CorePoolSize(), self.minPoolSize, err.Error())
			} else {
				idleClient := &IdleClient{flumeclient: fclient, expiredTime: (time.Now().Add(self.idletime))}
				self.idlePool.PushFront(idleClient)
			}
		}
	}

	return fclient
}

func (self *FlumeClientPool) Destroy() {
	self.mutex.Lock()
	self.running = false
	self.mutex.Unlock()
	for {
		time.Sleep(5 * time.Second)
		if self.ActivePoolSize() <= 0 {

			break
		}
		log.Printf("flume client pool closing : activepool:%d\n", self.ActivePoolSize())
	}

	//关闭掉空闲的client
	for e := self.idlePool.Front(); e != nil; e = e.Next() {
		fclient := e.Value.(*IdleClient)
		fclient.flumeclient.Destroy()
	}
	//关闭掉已经
	for e := self.checkOutPool.Front(); e != nil; e = e.Next() {
		e.Value.(*client.FlumeClient).Destroy()
	}

}
