package main

import (
	"github.com/lpflpf/goschedule/common/config"
	"github.com/lpflpf/goschedule/common/zookeeper"
	"github.com/lpflpf/goschedule/console/actions"
)

func main() {
	config.SetConfigDir("./config")
	initZk()
	actions.Start()
}

func initZk() {
	zkConfig := &config.ZookeeperConfig{}
	config.NewConfig("zk.json").Load(&zkConfig)
	zookeeper.SetZkConfig(zkConfig)
}
