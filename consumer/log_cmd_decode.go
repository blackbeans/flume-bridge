package consumer

import (
	"encoding/json"
	"flume-bridge/config"
	"flume-bridge/consumer/client"
	"flume-bridge/rpc/flume"
	"fmt"
	"log"
)

//解析出decodecommand
func decodeCommand(resp []byte) (string, *flume.ThriftFlumeEvent) {
	var cmd config.Command
	err := json.Unmarshal(resp, &cmd)
	if nil != err {
		log.Printf("command unmarshal fail ! %T | error:%s\n", resp, err.Error())
		return "", nil
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
		return businessName, nil
	}

	//拼Body
	flumeBody := fmt.Sprintf("%s\t%s\t%s", momoid, action, string(body))
	obj := client.NewFlumeEvent()
	event := client.EventFillUp(obj, businessName, action, []byte(flumeBody))
	return businessName, event
}
