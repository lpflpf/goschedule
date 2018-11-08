package tasks

import (
	"encoding/json"
	"github.com/lpflpf/goschedule/common/config"
	"github.com/lpflpf/goschedule/common/zookeeper"
)

// 在task节点下manager 的信息,为多个manager的instanceId，使用struct, 便于后期扩展

type TMData struct {
	Managers map[string]struct{}
}


func (tMData *TMData) RsyncToZk(task *Task, args ...*zookeeper.Zookeeper) error {
	var conn *zookeeper.Zookeeper
	if len(args) == 0 {
		conn = zookeeper.GetZkConn()
		defer conn.Close()
	} else {
		conn = args[0]
	}

	if data, err := json.Marshal(tMData); err != nil {
		return err
	} else {
		if _, err := conn.Set(config.GetTaskManagerPath(task.GroupId, task.Id), data); err != nil {
			return err
		}
	}

	return nil
}

func (tMData *TMData) DropManager(task *Task, managerId string, args ...*zookeeper.Zookeeper) error {
	delete(tMData.Managers, managerId)
	tMData.RsyncToZk(task)
	return nil
}

func GetTaskManagerData(task *Task, args ...*zookeeper.Zookeeper) (*TMData, error) {
	var conn *zookeeper.Zookeeper
	if len(args) == 0 {
		conn = zookeeper.GetZkConn()
		defer conn.Close()
	} else {
		conn = args[0]
	}

	path := config.GetTaskManagerPath(task.GroupId, task.Id)
	if data, _, err := conn.Get(path); err == nil {
		taskManagerData := &TMData{}
		json.Unmarshal(data, taskManagerData)
		if taskManagerData.Managers == nil {
			taskManagerData.Managers = make(map[string]struct{})
		}
		return taskManagerData, nil
	} else {
		return nil, err
	}
}
