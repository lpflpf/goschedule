package models

import (
	"github.com/lpflpf/goschedule/common/zookeeper"
	"github.com/lpflpf/goschedule/common/tasks"
)

func GetTaskInfo(task *tasks.Task) *tasks.Task {
	task.RsyncTaskFromZk(zookeeper.GetZkConn())
	return task
}

func GetTaskManagers(task *tasks.Task) *tasks.TMData {
	tMData, _ := tasks.GetTaskManagerData(task, zookeeper.GetZkConn())
	return tMData
}

func GetTaskWorkers(task *tasks.Task) []string {
	children, _ := task.GetTaskWorkers(zookeeper.GetZkConn())
	return children
}
