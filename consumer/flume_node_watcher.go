package consumer

import (
	"flume-log-sdk/config"
	"flume-log-sdk/consumer/pool"
	"log"
)

type FlumeWatcher struct {
	sourcemanger *SourceManager
	business     string
}

func newFlumeWatcher(business string, sourcemanger *SourceManager) *config.Watcher {
	flumeWatcher := &FlumeWatcher{business: business, sourcemanger: sourcemanger}
	return config.NewWatcher(business, flumeWatcher)
}

func (self *FlumeWatcher) BusinessWatcher(business string, eventType config.ZkEvent) {
	//当前节点有发生变更,只关注删除该节点就行
	if eventType == config.Deleted {
		self.sourcemanger.mutex.Lock()
		defer self.sourcemanger.mutex.Unlock()
		val, ok := self.sourcemanger.sourceServers[business]
		if ok {
			//关闭这个业务消费
			val.stop()
			delete(self.sourcemanger.sourceServers, business)
			for e := val.flumeClientPool.Back(); nil != e; e = e.Prev() {
				self.clearPool(business, e.Value.(*pool.FlumePoolLink))
			}

			log.Printf("business:[%s] deleted\n", business)
		} else {
			log.Printf("business:[%s] not exist !\n", business)
		}
	}
}

//清理掉pool
func (self *FlumeWatcher) clearPool(business string, pool *pool.FlumePoolLink) {
	pool.Mutex.Lock()
	if pool.BusinessLink.Len() == 0 {
		//如果已经没有使用的业务了直接关掉该pool
		pool.FlumePool.Destroy()
		hp := pool.FlumePool.GetHostPort()
		delete(self.sourcemanger.hp2flumeClientPool, pool.FlumePool.GetHostPort())
		log.Printf("remove flume agent :[%s]", hp)
	}
	pool.Mutex.Unlock()
}

func (self *FlumeWatcher) ChildWatcher(business string, childNode []config.HostPort) {
	//当前业务下的flume节点发生了变更会全量推送一次新的节点

	if len(childNode) <= 0 {
		self.BusinessWatcher(business, config.Deleted)
		return
	}

	self.sourcemanger.mutex.Lock()
	defer self.sourcemanger.mutex.Unlock()
	val, ok := self.sourcemanger.sourceServers[business]
	if ok {
		//判断该业务下已经被停掉的节点
		// for _, link := range val.flumeClientPool {
		for e := val.flumeClientPool.Back(); nil != e; e = e.Next() {
			link := e.Value.(*pool.FlumePoolLink)
			hp := link.FlumePool.GetHostPort()
			contain := false
			for _, chp := range childNode {
				//如果当前节点没有变更，即存在在childnode中不变更
				if hp == chp {
					contain = true
				}
			}

			//如果当前的node没有在childnode中则删除该pooL
			if !contain {
				link.DetachBusiness(business)
				self.clearPool(business, link)
				//从Business的clientpool中移除该client
				val.flumeClientPool.Remove(e)
				log.Printf("business:%s|WATCHER|REMOVE FLUME:%s", business, hp)
			}
		}

		//已经存在那么就检查节点变更
		for _, hp := range childNode {
			//先创建该业务节点：
			fpool, ok := self.sourcemanger.hp2flumeClientPool[hp]
			//如果存在Pool直接使用
			if ok {
				//检查该业务已有是否已经该flumepool
				//如果不包含则创建该池子并加入该业务对应的flumeclientpoollink中
				if !fpool.IsAttached(business) {
					val.flumeClientPool.PushFront(fpool)
					fpool.AttachBusiness(business)
					log.Printf("business:[%s] add flume :[\n", business, fpool)
				}
				//如果已经包含了，则啥事都不干

			} else {
				//如果不存在该flumepool，直接创建并且添加到该pool种
				err, poollink := pool.NewFlumePoolLink(hp)
				if nil != err {
					self.sourcemanger.hp2flumeClientPool[hp] = poollink
					val.flumeClientPool.PushFront(poollink)
					poollink.AttachBusiness(business)
				}
			}
		}

	} else {
		sourceserver := self.sourcemanger.initSourceServer(business, childNode)
		sourceserver.start()
	}
}
