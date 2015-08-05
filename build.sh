#!/bin/bash
go build flume-bridge/consumer/client
go build flume-bridge/config 

go build flume-bridge/consumer/pool
go build flume-bridge/consumer


go install flume-bridge/config 
go install flume-bridge/consumer/client
go install flume-bridge/consumer/pool
go install flume-bridge/consumer

VERSION=`date +%Y%m%d%H`
# go build  -o /home/server/flume-bin/flume-log-$VERSION flume-log.go

go build flume-log.go
