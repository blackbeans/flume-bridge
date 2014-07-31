package config

import (
	"github.com/blackbeans/zk"
	"log"
	"testing"
)

func Test_ZKManager(t *testing.T) {
	zkhost := "localhost:2181"
	zkmanager := NewZKManager(zkhost)

	flumenode := zkmanager.GetAndWatch("business",
		func(path string, eventType ZkEvent) {
			t.Logf("business node event %s %s", path, eventType)
		},
		func(path string, childNode []HostPort) {
			t.Logf("child changed %s|%s", path, childNode)
		})

	if nil != flumenode {
		t.Fail()
	}

	// netport, _ := net.InterfaceByName("en0")
	// addrs, _ := netport.Addrs()
	// var addr string
	// for _, v := range addrs {
	// 	if strings.Contains(v.String(), ".") {
	// 		addr = v.String()
	// 		break
	// 	}
	// }

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

	flumenode = zkmanager.GetAndWatch("business",
		func(path string, eventType ZkEvent) {

		},
		func(path string, childNode []HostPort) {
			t.Logf("%s|%s", path, childNode)
		})

	t.Logf("flumenode:%s", flumenode)

	defer zkmanager.Close()

}
