package consumer

import (
	"flume-log-sdk/config"
	"fmt"
	"sort"
	"strconv"
	"time"
)

func (self *SourceManager) monitor() {

	for self.isRunning {
		now := time.Now()
		nt := now.Format("2006-01-02 15:04:05")
		time.Sleep(1 * time.Second)

		self.monitorFlumeTPS(nt)
		self.monitorFlumePool(nt)
		self.monitorRedis(nt)
	}
}

func (self *SourceManager) monitorFlumeTPS(nt string) {
	//---------------flumetps-----------
	mk := make([]string, 0)
	for k, v := range self.sourceServers {
		succ, fail, bufferSize := v.monitor()
		item := fmt.Sprintf("%s|%d/%d/%d \t", k, succ, fail, bufferSize)
		mk = append(mk, item)
	}

	sort.Strings(mk)
	monitor := nt + "\t"
	for _, v := range mk {
		monitor += v + "\t"
	}
	self.flumeLog.Println(monitor)
}

func (self *SourceManager) monitorFlumePool(nt string) {

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
		monitor := nt + "|" + hp + "\n"
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

func (self *SourceManager) monitorRedis(nt string) {
	monitor := nt + "\t"
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
