package consumer

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
	flumePool *flumeClientPool

	businessLink *list.List //使用这个clientpool的业务名称

	mutex sync.Mutex //保证在并发情况下能够对list的操作安全
}

func newFlumePoolLink(hp config.HostPort) *FlumePoolLink {
	pool := newFlumeClientPool(20, 50, 100, 10*time.Second, func() *client.FlumeClient {
		flumeclient := client.NewFlumeClient(hp.Host, hp.Port)
		flumeclient.Connect()
		return flumeclient
	})
	//将此pool封装为Link
	return &FlumePoolLink{flumePool: pool, businessLink: list.New()}
}

//flume连接池
type flumeClientPool struct {
	dialFunc     func() *client.FlumeClient
	maxPoolSize  int //最大尺子大小
	minPoolSize  int //最小连接池大小
	corepoolSize int //核心池子大小
	// activePoolSize int //当前正在运行的client

	idletime time.Duration //空闲时间

	idlePool *list.List //连接的队列

	checkOutPool *list.List //已经获取的poolsize

	mutex sync.Mutex
}

type IdleClient struct {
	flumeclient *client.FlumeClient
	expiredTime time.Time
}

func newFlumeClientPool(minPoolSize, corepoolSize, maxPoolSize int, idletime time.Duration, dialFunc func() *client.FlumeClient) *flumeClientPool {

	idlePool := list.New()
	checkOutPool := list.New()
	clientpool := &flumeClientPool{
		maxPoolSize:  maxPoolSize,
		corepoolSize: corepoolSize,
		minPoolSize:  minPoolSize,
		idletime:     idletime,
		idlePool:     idlePool,
		dialFunc:     dialFunc,
		checkOutPool: checkOutPool}

	clientpool.mutex.Lock()
	defer clientpool.mutex.Unlock()
	//初始化一下最小的Poolsize,让入到idlepool中
	for i := 0; i < clientpool.minPoolSize; i++ {
		idleClient := &IdleClient{flumeclient: dialFunc(), expiredTime: (time.Now().Add(clientpool.idletime))}
		clientpool.idlePool.PushFront(idleClient)
		// clientpool.activePoolSize++
	}

	return clientpool
}

func (self *flumeClientPool) Get(timeout time.Duration) (*client.FlumeClient, error) {

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
func (self *flumeClientPool) CorePoolSize() int {
	return self.idlePool.Len() + self.checkOutPool.Len()

}

func (self *flumeClientPool) ActivePoolSize() int {
	return self.checkOutPool.Len()
}

//释放坏的资源
func (self *flumeClientPool) ReleaseBroken(fclient *client.FlumeClient) error {
	self.mutex.Lock()
	defer self.mutex.Unlock()
	_, err := self.innerRelease(fclient)
	return err

}

func (self *flumeClientPool) innerRelease(fclient *client.FlumeClient) (bool, error) {
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
func (self *flumeClientPool) Release(fclient *client.FlumeClient) error {

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
func (self *flumeClientPool) innerGet() *client.FlumeClient {

	var fclient *client.FlumeClient
	//首先检查一下当前空闲连接中是否有需要关闭的
	self.mutex.Lock()
	defer self.mutex.Unlock()
	for back := self.idlePool.Back(); back != nil; back = back.Prev() {

		// push ---> front ----> back 最旧的client
		idle := (back.Value).(*IdleClient)

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
			log.Printf("client pool is full ! minPoolSize:%d,maxPoolSize:%d,corePoolSize:%d,activePoolSize:%d ",
				self.minPoolSize, self.maxPoolSize, self.CorePoolSize(), self.ActivePoolSize())
			return fclient
		} else {
			//如果没有可用链接则创建一个
			newClient := self.dialFunc()
			self.checkOutPool.PushFront(newClient)
			fclient = newClient
			// self.activePoolSize++
		}
	}

	return fclient
}

func (self *flumeClientPool) Destroy() {

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
