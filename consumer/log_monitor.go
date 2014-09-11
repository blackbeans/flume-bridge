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
		now := time.Now()
		nt := now.Format("2006-01-02 15:04:05")
		time.Sleep(1 * time.Second)
		//---------------flumetps-----------
		mk := make([]string, 0)

		for k, v := range self.sourceServers {
			succ, fail, bufferSize := v.monitor()
			item := fmt.Sprintf("%s|%d/%d/%d \t", k, succ, fail, bufferSize)
			mk = append(mk, item)
		}

		sort.Strings(mk)
		monitor := ""
		if now.Second()%10 == 0 {
			monitor += "------------------------" + nt + "------------------------\n"
		}

		for _, v := range mk {
			monitor += v
		}
		self.flumeLog.Println(monitor)

		//---------------flumepool-----------
		mk = make([]string, 0)

		for k, _ := range self.hp2flumeClientPool {

			item := k.Host + ":" + strconv.Itoa(k.Port)
			mk = append(mk, item)

		}
		sort.Strings(mk)

		monitor = ""
		if now.Second()%10 == 0 {
			monitor += "------------------------" + nt + "------------------------\n"
		}
		i := 0
		for _, hp := range mk {
			v, ok := self.hp2flumeClientPool[config.NewHostPort(hp)]
			if !ok {
				continue
			}
			i++
			active, core, max := v.FlumePool.MonitorPool()
			monitor += fmt.Sprintf("%s|%d/%d/%d\t", hp, active, core, max)
			if i%5 == 0 {
				monitor += "\n"
			}
		}

		self.flumePoolLog.Println(monitor)
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
