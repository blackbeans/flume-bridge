package consumer

import (
	"flume-bridge/config"
	"fmt"
	"sort"
	"strconv"
	"time"
)

func (self *SourceManager) monitor() {

	for self.isRunning {
		time.Sleep(1 * time.Second)
		self.monitorFlumeTPS()
		self.monitorFlumePool()
		self.monitorRedis()
	}
}

func (self *SourceManager) monitorFlumeTPS() {
	//---------------flumetps-----------
	mk := make([]string, 0)
	for k, v := range self.sourceServers {
		succ, fail, bufferSize, arrayPool := v.monitor()
		item := fmt.Sprintf("%s|%d/%d/%d/%d \t", k, succ, fail, bufferSize, arrayPool)
		mk = append(mk, item)
	}

	sort.Strings(mk)
	monitor := ""
	for _, v := range mk {
		monitor += v + "\t"
	}
	self.flumeLog.Println(monitor)
}

func (self *SourceManager) monitorFlumePool() {

	//---------------flumepool-----------
	mk := make(map[string][]int, 0)
	hosts := make([]string, 0)
	for k, _ := range self.hp2flumeClientPool {

		ports, ok := mk[k.Host]
		if !ok {
			ports = make([]int, 0)
			hosts = append(hosts, k.Host)
		}
		ports = append(ports, k.Port)
		mk[k.Host] = ports
	}
	sort.Strings(hosts)
	for _, hp := range hosts {
		i := 0
		monitor := hp + "\n"
		ports, _ := mk[hp]
		for _, port := range ports {
			v, ok := self.hp2flumeClientPool[config.NewHostPort(hp+":"+strconv.Itoa(port))]
			if !ok {
				continue
			}

			i++
			active, core, max := v.FlumePool.MonitorPool()
			monitor += fmt.Sprintf("%d:%d/%d/%d\t", port, active, core, max)
			if i%5 == 0 {
				monitor += "\n"
			}
		}
		self.flumePoolLog.Println(monitor)
	}

}

func (self *SourceManager) monitorRedis() {
	monitor := ""
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
