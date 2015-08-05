package consumer

import (
	"flume-bridge/config"
	"flume-bridge/consumer/pool"
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
			for _, p := range val.clientPools {
				self.clearPool(business, p)
			}
			self.sourcemanger.watcherLog.Printf("business:[%s] deleted\n", business)
		} else {
			self.sourcemanger.watcherLog.Printf("business:[%s] not exist !\n", business)
		}
	}
}

//清理掉pool
func (self *FlumeWatcher) clearPool(business string, pool *pool.FlumePoolLink) {
	pool.Mutex.Lock()
	defer pool.Mutex.Unlock()
	if pool.BusinessLink.Len() == 0 {
		//如果已经没有使用的业务了直接关掉该pool
		pool.FlumePool.Destroy()
		hp := pool.FlumePool.GetHostPort()
		delete(self.sourcemanger.hp2flumeClientPool, pool.FlumePool.GetHostPort())
		self.sourcemanger.watcherLog.Printf("WATCHER|REMOVE FLUME:%s\n", hp)
	}

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

	sourceserver := self.sourcemanger.initSourceServer(business, childNode)
	sourceserver.start()

	//存在的话就直接stop再重新创建一个
	if ok {
		val.stop()
	}
	self.sourcemanger.sourceServers[business] = sourceserver
}
