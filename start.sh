#!/bin/bash

GOGCTRACE=1 
nohup ./flume-log -redis=localhost:6379 -instancename=flume-test -zkhost=localhost:2181 --queuename=new-log -logpath=. 2>&1 >stdout.log &
