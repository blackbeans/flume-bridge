package consumer

//启动设置的选项
type Option struct {
	flumeHostName string

	flumePort int32

	host string

	port int32
}

func NewOption(flumeHostName string, flumePort int32, host string, port int32) *Option {

	return &Option{flumeHostName: flumeHostName, flumePort: flumePort, host: host, port: port}
}
