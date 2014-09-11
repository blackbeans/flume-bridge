package consumer

import (
	"flume-log-sdk/config"
	"fmt"
	"sort"
	"strconv"
	"time"
)

func (self *SourceManager) monitorFlume() {

	for self.isRunning {
		time.Sleep(1 * time.Second)
		monitor := "FLUME_TPS|"
		for k, v := range self.sourceServers {

			succ, fail := v.monitor()
			monitor += fmt.Sprintf("%s|%d/%d \t", k, succ, fail)
		}

		self.flumeLog.Println(monitor)

		mk := make([]string, 0)
		monitor = "FLUME_POOL|"
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
			monitor += fmt.Sprintf("%s|%d/%d/%d ", hp, active, core, max)
		}

		self.flumeLog.Println(monitor)
		self.monitorRedis()
	}
}

func (self *SourceManager) monitorRedis() {
	monitor := "REDIS_TPS|"
	for k, v := range self.redispool {
		//队列K
		monitor += k
		for _, poolw := range v {
			cost := (poolw.currValue - poolw.lastValue)
			poolw.lastValue = poolw.currValue
			monitor += fmt.Sprintf("|%s|%d \t", poolw.hostport.Host+":"+strconv.Itoa((poolw.hostport.Port)), cost)
		}
	}
	self.redisLog.Println(monitor)
}
