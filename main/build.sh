#!/bin/bash
VERSION=`date -d last-day +%Y%m%d`
go build  -o /home/server/flume-bin/flume-log-$VERSION flume-log.go
