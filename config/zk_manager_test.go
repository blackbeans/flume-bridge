package config

import (
	"github.com/blackbeans/zk"
	"log"
	"os"
	"strconv"
	"testing"
	"time"
)

type TestWatcher struct {
}

func (self *TestWatcher) BusinessWatcher(path string, eventType ZkEvent) {
	log.Printf("----------business node event %s %s\n", path, eventType)
}

func (self *TestWatcher) ChildWatcher(path string, childNode []HostPort) {
	log.Printf("++++++++++child changed %s|%s\n", path, childNode)
}

func Test_ZKManager(t *testing.T) {
	zkhost := "localhost:2181"
	zkmanager := NewZKManager(zkhost)
	watcher := NewWatcher("business", &TestWatcher{})
	flumenode := zkmanager.GetAndWatch("business", watcher)

	zkmanager.GetAndWatch("location", watcher)

	if nil != flumenode {
		t.Fail()
	}

	ip := "192.168.0.101"
	if len(ip) > 0 {
		ip += "_8080"
		node := FLUME_PATH + "/business/" + ip + "_"
		resp, err := zkmanager.session.Create(node, nil, zk.CreateSequence, zk.AclOpen)
		if nil != err {
			log.Println("create path fail! " + node + "\t" + err.Error())
			// t.Fail()

		} else {
			log.Println(resp)
		}

	}

	flumenode = zkmanager.GetAndWatch("business", watcher)

	t.Logf("flumenode:%s", flumenode)

	hostname, _ := os.Hostname()
	pid := os.Getpid()
	zkmanager.RegistePath([]string{"node"}, hostname+"_"+strconv.Itoa(pid))

	exist, _, _ := zkmanager.session.Exists(FLUME_SOURCE_PATH_PID+"/node/"+hostname+"_"+strconv.Itoa(pid), nil)
	if !exist {
		t.Failed()
	} else {

	}
	time.Sleep(10 * time.Second)
	defer zkmanager.Close()

}
