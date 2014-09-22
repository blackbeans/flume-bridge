#!/bin/bash
go build ../../flume-log-sdk/config 
go build ../../flume-log-sdk/consumer/client
go build ../../flume-log-sdk/consumer/pool
go build ../../flume-log-sdk/consumer


go install ../../flume-log-sdk/config 
go install ../../flume-log-sdk/consumer/client
go install ../../flume-log-sdk/consumer/pool
go install ../../flume-log-sdk/consumer

VERSION=`date +%Y%m%d%H`
go build  -o /home/server/flume-bin/flume-log-$VERSION flume-log.go
