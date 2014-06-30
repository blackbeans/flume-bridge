package consumer

type HostPort struct {
	Host string
	Port int
}

type QueueHostPort struct {
	HostPort
	QueueName string

	Maxconn int

	Timeout int
}

//启动设置的选项
type Option struct {
	flumeAgents []HostPort

	queueHostPorts []QueueHostPort //redis队列Pop

}

func NewOption(flumeAgents []HostPort, hostPorts []QueueHostPort) *Option {

	return &Option{flumeAgents: flumeAgents, queueHostPorts: hostPorts}
}

//command
type command struct {
	Action string `json:"action"`

	Params map[string]string `json:"params"`
}
