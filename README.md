flume-log-sdk
=============

一个使用redis队列及集成thrift协议、用zookeeper保证flume节点高可用的flume客户端。

=============
优点：
* 1.支持flume-log的多业务水平扩展
* 2.支持flme-log的单业务的垂直扩展
* 3.具有flume-log节点挂掉后自动发现（告警）
* 4.flume-node的节点动态感知、无忧重启flume node
* 5. thrift 连接池管理
* 6.自定义的发送路由规则(当前为round-robin)





