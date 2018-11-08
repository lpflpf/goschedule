package manager

import (
	"github.com/lpflpf/goschedule/common/tasks"
	"github.com/qiniu/log"
	"github.com/samuel/go-zookeeper/zk"
)

type MsgType int

type MessageDeal struct {
	manager *Manager
}

func (deal *MessageDeal) AddWatch(task *tasks.Task, reply *int) error {
	log.Info("add watch by dial", task)
	manager := deal.manager
	manager.Data.WatchTasks = append(manager.Data.WatchTasks, task)
	if err := manager.Data.RsyncToZk(); err != nil {
		*reply = 1
		return nil
	}
	manager.watchTaskAndReady(task)
	manager.Data.WatchTask(task)
	manager.AppendManager2Task(task)
	log.Info("[Send Msg Worker Add]")
	manager.chEvent <- Event{
		eventType: WorkerAdd,
		message:   WorkerModifyMessage{[]string{}, zk.Event{}, task.GroupId, task.Id},
	}
	*reply = 1
	return nil
}

func (deal *MessageDeal) UnWatch(task *tasks.Task, replay *int) error {
	deal.manager.UnWatchTask(task)
	return nil
}
