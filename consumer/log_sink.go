package consumer

import (
	"encoding/json"
	"flume-log-sdk/config"
	"flume-log-sdk/consumer/client"
	"fmt"
	"github.com/blackbeans/redigo/redis"
	"log"
	"math/rand"
	_ "os"
	"strconv"
	"time"
)

// 用于向flume中作为sink 通过thrift客户端写入日志

type SinkServer struct {
	redisPool       map[string][]*redis.Pool
	flumeClientPool []*flumeClientPool
	isStop          bool
}

func NewSinkServer(option *config.Option) (server *SinkServer) {

	redisPool := make(map[string][]*redis.Pool, 0)

	//创建redis的消费连接
	for _, v := range option.QueueHostPorts {

		pool := redis.NewPool(func() (conn redis.Conn, err error) {

			conn, err = redis.DialTimeout("tcp", v.Host+":"+strconv.Itoa(v.Port),
				time.Duration(v.Timeout)*time.Second,
				time.Duration(v.Timeout)*time.Second,
				time.Duration(v.Timeout)*time.Second)

			return
		}, time.Duration(v.Timeout*2)*time.Second, v.Maxconn/2, v.Maxconn)

		pools, ok := redisPool[v.QueueName]
		if !ok {
			pools = make([]*redis.Pool, 0)
			redisPool[v.QueueName] = pools
		}

		redisPool[v.QueueName] = append(pools, pool)

	}

	pools := make([]*flumeClientPool, 0)
	//创建flume的client
	for _, v := range option.FlumeAgents {

		pool := newFlumeClientPool(20, 50, 100, 10*time.Second, func() *client.FlumeClient {
			flumeclient := client.NewFlumeClient(v.Host, v.Port)
			flumeclient.Connect()
			return flumeclient
		})
		pools = append(pools, pool)

		go monitorPool(v.Host+":"+strconv.Itoa(v.Port), pool)
	}

	sinkserver := &SinkServer{redisPool: redisPool, flumeClientPool: pools}

	return sinkserver
}

func monitorPool(hostport string, pool *flumeClientPool) {
	for {
		time.Sleep(1 * time.Second)
		log.Printf("flume:%s|active:%d,core:%d,max:%d",
			hostport, pool.ActivePoolSize(), pool.CorePoolSize(), pool.maxPoolSize)
	}

}

//启动pop
func (self *SinkServer) Start() {

	self.isStop = false
	ch := make(chan int, 1)
	var count = 0
	for k, v := range self.redisPool {

		log.Println("start redis queueserver succ " + k)
		for _, pool := range v {
			count++
			defer pool.Close()
			go func(queuename string, pool *redis.Pool, end chan int) {
				conn := pool.Get()
				defer pool.Release(conn)
				for !self.isStop {

					// log.Println("pool active count :", strconv.Itoa(pool.ActiveCount()))
					reply, err := conn.Do("LPOP", queuename)
					if nil != err || nil == reply {
						if nil != err {
							log.Printf("LPOP|FAIL|%s", err)
							conn.Close()
							conn = pool.Get()
						} else {
							time.Sleep(100 * time.Millisecond)
						}

						continue
					}

					resp := reply.([]byte)
					var cmd config.Command
					err = json.Unmarshal(resp, &cmd)

					if nil != err {
						log.Printf("command unmarshal fail ! %s | error:%s\n", resp, err.Error())
						continue
					} else if rand.Int()%10 == 0 {
						log.Println("trace|command|%s", cmd)
					}

					//
					momoid := cmd.Params["momoid"].(string)

					businessName := cmd.Params["businessName"].(string)

					action := cmd.Params["type"].(string)

					bodyContent := cmd.Params["body"]

					//将businessName 加入到body中
					bodyMap := bodyContent.(map[string]interface{})
					bodyMap["business_type"] = businessName

					body, err := json.Marshal(bodyContent)

					if nil != err {
						log.Printf("marshal log body fail %s", err.Error())
						continue
					}

					//这里需要优化一下body,需要采用其他的方式定义Body格式，写入

					// log.Printf("%s,%s,%s,%s", momoid, businessName, action, string(body))

					//启动处理任务
					go self.innerSend(momoid, businessName, action, string(body))

				}
				end <- -1
			}(k, pool, ch)
		}
	}

	for {
		count += <-ch
		if count <= 0 {
			log.Printf("redis conn  close %d", count)
			break
		}
	}

}

func (self *SinkServer) innerSend(momoid, businessName, action string, body string) {

	for i := 0; i < 3; i++ {
		pool := self.getFlumeClientPool(businessName, action)
		flumeclient, err := pool.Get(5 * time.Second)
		if nil != err {
			continue
		}
		//拼装头部信息
		header := make(map[string]string, 1)
		header["businessName"] = businessName
		header["type"] = action

		//拼Body
		flumeBody := fmt.Sprintf("%s\t%s\t%s", momoid, action, body)
		err = flumeclient.Append(header, []byte(flumeBody))
		defer func() {
			if err := recover(); nil != err {
				//回收这个坏的连接
				pool.ReleaseBroken(flumeclient)
			}
		}()

		if nil != err {
			log.Printf("send 2 flume fail %s \t err:%s\n", body, err.Error())
		} else {
			// log.Printf("send 2 flume succ %s\n", body)
			pool.Release(flumeclient)
			break
		}
	}
}

//仅供测试使用推送数据
func (self *SinkServer) testPushLog(queuename, logger string) {

	for _, v := range self.redisPool {
		for _, pool := range v {
			conn := pool.Get()
			defer pool.Release(conn)

			reply, err := conn.Do("RPUSH", queuename, logger)
			log.Printf("%s|err:%s", reply, err)
			break

		}
	}

}

func (self *SinkServer) Stop() {
	self.isStop = true
	for _, v := range self.flumeClientPool {
		v.Destroy()
	}

	for _, v := range self.redisPool {
		for _, p := range v {
			p.Close()
		}
	}
}

func (self *SinkServer) getFlumeClientPool(businessName, action string) *flumeClientPool {

	//使用随机算法直接获得

	idx := rand.Intn(len(self.flumeClientPool))
	return self.flumeClientPool[idx]

}
