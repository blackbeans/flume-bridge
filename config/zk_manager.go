package config

import (
	_ "github.com/blackbeans/zk"
)

//配置的flumenode
type FlumeNode struct {
	businessName string   `json:'business'`
	flume        HostPort `json:'flume'`
}

/**
* 从zk上获取集群配置
*
**/

func FetchConfigFromZk() map[string][]FlumeNode {

}
