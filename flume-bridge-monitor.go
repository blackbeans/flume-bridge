package main

import (
	"fmt"
	"github.com/blackbeans/goquery"
	"log"
	"net/http"
	"net/url"
	"time"
)

func main() {

	flumes := []string{"flume001.m6", "flume002.m6",
		"flume-log-001.m6", "flume-log-002.m6", "flume-log-003.m6",
		"flume-log-004.m6", "flume-log-005.m6", "flume-log-006.m6"}

	for {
		scan(flumes)
		log.Printf(time.UTC.String() + "\t 扫描意外终止的flume结束....\n")
		time.Sleep(1 * time.Minute)
	}

}

/**
 * supervisor实例
 */
type SupervisorInstance struct {
	Host   string `json:"host"`   //当前机器名
	Name   string `json:"name"`   //服务名称
	Status string `json:"status"` //当前状态
	Info   string `json:"info"`   //启动信息
}

const (
	START_URL = "index.html?processname=%s&action=start"
)

func scan(hosts []string) {
	for _, v := range hosts {
		baseUrl := "http://" + v + ":9001"
		doc, err := goquery.NewDocument(baseUrl)
		if nil == err {

			exitInstance := make([]string, 0, 1)
			doc.Find("table tbody tr").Each(func(i int, s *goquery.Selection) {
				instance := SupervisorInstance{Host: v}
				s.Find("td").Each(func(j int, ss *goquery.Selection) {
					if j > 2 {
						return
					}
					switch j {
					case 0:
						instance.Status = ss.Children().Text()
					case 1:
						instance.Info = ss.Children().Text()
					case 2:
						instance.Name = ss.Children().Text()
					}
				})
				if instance.Status == "exited" {
					exitInstance = append(exitInstance, instance.Name)
				}
			})

			alarm := "host:" + v + " flume节点restart:["

			for i, inst := range exitInstance {
				//发送告警，并重启
				url := fmt.Sprintf("%s/"+START_URL, baseUrl, inst)
				resp, err := http.Get(url)
				if nil != err {
					log.Printf("重启失败:%s|%s", url, err)
					alarm += inst + ":fail,"
					continue
				}
				defer resp.Body.Close()
				alarm += inst + ":succ,"
			}

			alarm += "]"

			//如果有坏掉的节点则发送告警
			if len(exitInstance) > 0 {
				alarmUrl := fmt.Sprintf("http://monitor001.m6:8001/alarmproxy?host=%s&action=%s&msg=%s&status=1&level=1&timestamp=%d",
					v, "flume_node", url.QueryEscape(alarm), time.Now().Unix())

				// log.Println(alarmUrl)
				resp, err := http.Get(alarmUrl)
				if nil != err {
					log.Printf("发送告警失败:%s|%s", alarmUrl, err)
					continue
				}
				defer resp.Body.Close()
			}
		}
	}
}
