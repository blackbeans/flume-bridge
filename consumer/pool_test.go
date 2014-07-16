package consumer

import (
	"container/list"
	"flume-log-sdk/consumer/client"
	"log"
	"testing"
	"time"
)

func Test_Pool(t *testing.T) {
	clientPool := newFlumeClientPool(10, 20, 30, 10*time.Second, func() *client.FlumeClient {
		flumeclient := client.NewFlumeClient("localhost", 44444)
		flumeclient.Connect()
		return flumeclient
	})

	idlecount := clientPool.CorePoolSize()

	//默认初始化10个最小连接
	if idlecount != 10 {
		t.Fail()
	}

	var err error
	checkout := list.New()
	for i := 0; i < 45; i++ {
		fclient, perr := clientPool.Get(5 * time.Second)
		if nil == perr {
			checkout.PushFront(fclient)
		}

		err = perr
	}

	if nil == err {
		//达到最大的连接数后应该会报错
		t.Fail()
	}

	log.Printf("------------active:%d,core:%d,max:%d", clientPool.ActivePoolSize(), clientPool.CorePoolSize(), clientPool.maxPoolSize)
	//如果活动线程数等于现在持有的则成功，反则失败
	if clientPool.ActivePoolSize() != checkout.Len() {
		t.Fail()
	}

	if clientPool.CorePoolSize() != clientPool.maxPoolSize {
		t.Fail()
	}

	func() {

		for e := checkout.Front(); nil != e; e = e.Next() {
			clientPool.Release(e.Value.(*client.FlumeClient))
		}
	}()

	log.Printf("release  conn ------------active:%d,core:%d,max:%d", clientPool.ActivePoolSize(), clientPool.CorePoolSize(), clientPool.maxPoolSize)

	for i := 0; i < 20; i++ {

		client, err := clientPool.Get(5 * time.Second)
		log.Printf("------------active:%d,core:%d,max:%d", clientPool.ActivePoolSize(), clientPool.CorePoolSize(), clientPool.maxPoolSize)
		if nil != err {
			log.Println(err)
		} else {
			time.Sleep(2 * time.Second)
			clientPool.Release(client)
		}
	}

	log.Printf("end ------------active:%d,core:%d,max:%d", clientPool.ActivePoolSize(), clientPool.CorePoolSize(), clientPool.maxPoolSize)

	clientPool.Destroy()

}
