package main

import (
	"fmt"
	"github.com/lpflpf/goschedule/common/config"
	"github.com/lpflpf/goschedule/common/tasks"
	"github.com/lpflpf/goschedule/common/zookeeper"
	"github.com/lpflpf/goschedule/worker/worker"
	"github.com/qiniu/log"
	"time"
)

func main() {
	config.SetConfigDir("./config")
	task := &tasks.Task{}
	config.NewConfig("job.json").Load(&task)

	zkConfig := &config.ZookeeperConfig{}
	config.NewConfig("zk.json").Load(&zkConfig)
	zookeeper.SetZkConfig(zkConfig)
	fmt.Println(zkConfig)

	instance := worker.NewWorker(task)

	go func() {
		for {
			select {
			case <-instance.ChRunning:
				log.Info("worker Running")
			case <-instance.ChReBalance:
				log.Info("worker Balance")
			case <-instance.ChFinish:
				log.Info("worker Finish")
			case <-instance.ChStart:
				log.Info("worker Start")
			}
		}
	}()

	for {
		fmt.Println(instance.WData.Sharding)
		time.Sleep(1 * time.Second)
	}
}
