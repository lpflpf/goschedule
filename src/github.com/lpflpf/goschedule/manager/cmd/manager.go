package main

import (
	"flag"
	"fmt"
	"github.com/lpflpf/goschedule/common/config"
	"github.com/lpflpf/goschedule/common/zookeeper"
	"github.com/lpflpf/goschedule/manager/manager"
	"github.com/qiniu/log"
	"os"
	"runtime"
)

func main() {
	defer func() {
		if e := recover(); e != nil {
			var buf [4096]byte
			n := runtime.Stack(buf[:], false)
			fmt.Printf("==> %s\n", string(buf[:n]))
			os.Exit(1)
		}
	}()
	log.SetOutputLevel(log.Lerror)
	config.SetConfigDir("./config")
	initZk()

	managerConfigFilename := getParams()
	managerConfig := &config.ManagerConfig{}
	config.NewConfig(managerConfigFilename).Load(&managerConfig)
	manager.NewManager(managerConfig)
}

func initZk() {
	zkConfig := &config.ZookeeperConfig{}
	config.NewConfig("zk.json").Load(&zkConfig)
	zookeeper.SetZkConfig(zkConfig)
}

func getParams() string {
	var managerConf string
	flag.StringVar(&managerConf, "config-dir", "managers.json", "")
	flag.Parse()
	return managerConf
}
