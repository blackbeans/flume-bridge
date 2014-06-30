package consumer

import (
	"encoding/json"
	"fmt"
	"github.com/fzzy/radix/redis"
	"log"
	"os"
	"strconv"
	"time"
)

// 用于向flume中作为sink 通过thrift客户端写入日志

type SinkServer struct {
	queues       map[string][]*redis.Client
	flumeClients []*flumeClient
	isStop       bool
}

func NewSinkServer(option *Option) (server *SinkServer) {

	queues := make(map[string][]*redis.Client, 2)

	//创建redis的消费连接
	for _, v := range option.queueHostPorts {

		client, err := redis.DialTimeout("tcp", v.Host+":"+strconv.Itoa(v.Port), time.Duration(v.Timeout)*time.Second)
		if nil != err {
			log.Printf("open redis %s:%d fail!  %s\n", v.Host, v.Port, err.Error())
			os.Exit(-1)
		}

		clients, ok := queues[v.QueueName]
		if !ok {
			clients = make([]*redis.Client, 2)
			queues[v.QueueName] = clients
		}

		queues[v.QueueName] = append(clients, client)

	}

	flumeClients := make([]*flumeClient, 1)
	//创建flume的client
	for _, v := range option.flumeAgents {
		client := newFlumeClient(v.Host, v.Port)
		flumeClients = append(flumeClients, client)
	}

	sinkserver := &SinkServer{queues: queues, flumeClients: flumeClients}

	return sinkserver
}

//启动pop
func (self *SinkServer) start() {
	self.isStop = false
	for k, v := range self.queues {
		for _, conn := range v {
			go func(queuename string, conn *redis.Client) {
				for !self.isStop {
					cmdstr, err := conn.Cmd("LPOP", queuename).Bytes()
					if nil != err {
						log.Printf("LPOP|FAIL|%s", err.Error())
						time.Sleep(100 * time.Millisecond)
					}

					var cmd command

					err = json.Unmarshal(cmdstr, &cmd)
					if nil != err {
						log.Printf("command unmarshal fail ! %s | error:{%s}", string(cmdstr), err.Error())
						continue
					}

					//
					momoid, _ := cmd.Params["momoid"]

					businessName, _ := cmd.Params["businessName"]

					action, _ := cmd.Params["type"]

					body, _ := cmd.Params["body"]

					//启动处理任务
					go func(momoid, businessName, action, body string) {
						client := self.getFlumeClient(businessName, action)
						//拼装头部信息
						header := make(map[string]string, 1)
						header["businessName"] = businessName
						header["type"] = action

						//拼Body
						flumeBody := fmt.Sprintf("%s\t%s\t%s\n", momoid, action, body)
						err := client.append(header, []byte(flumeBody))

						if nil != err {
							log.Printf("send 2 flume fail %s\n", body)
						}

					}(momoid, businessName, action, body)

				}
			}(k, conn)
		}
	}
}

func (self *SinkServer) getFlumeClient(businessName, action string) *flumeClient {

	return self.flumeClients[0]
}
