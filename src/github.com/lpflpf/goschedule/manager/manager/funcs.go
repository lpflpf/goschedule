package manager

import (
	"encoding/json"
	"github.com/lpflpf/goschedule/common/config"
	"github.com/lpflpf/goschedule/common/tasks"
	"github.com/lpflpf/goschedule/common/zookeeper"
	"github.com/qiniu/log"
	"github.com/lpflpf/goschedule/common/work"
)

func GetWData(task *tasks.Task, instanceId string, args ...*zookeeper.Zookeeper) *work.WData {
	var conn *zookeeper.Zookeeper
	if len(args) == 0 {
		conn = zookeeper.GetZkConn()
		defer conn.Close()
	} else {
		conn = args[0]
	}
	path := config.GetWorkerInstancePath(task.GroupId, task.Id, instanceId)
	if data, _, err := conn.Get(path); err == nil {
		tmp := &work.WData{}
		if err := json.Unmarshal(data, tmp); err != nil {
			log.Error("Json Unmarshal failed, ", data)
		}
		return tmp
	} else {
		log.Error("get worker isntancePath failed.", err)
	}
	return nil
}
