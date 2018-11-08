package tasks

import (
	"encoding/json"
	"github.com/lpflpf/goschedule/common/zookeeper"
	"github.com/lpflpf/goschedule/common/config"
	"github.com/qiniu/log"
	"github.com/samuel/go-zookeeper/zk"
	"strings"
)

type Task struct {
	Id                 string
	GroupId            string
	ShardingTotalCount int
	Sharding           map[int]string
	Description        string
}

func NewTask(groupId string, id string) *Task {
	groupId = strings.Trim(groupId, "/")
	id = strings.Trim(id, "/")
	task := &Task{GroupId: groupId, Id: id}
	task.Sharding = make(map[int]string)
	return &Task{GroupId: groupId, Id: id}
}

func (task *Task) RsyncTaskToZk(conn *zookeeper.Zookeeper) error {
	log.Info("\033[31m [Rsync Task To Zk]\033[0m start...")
	path := config.GetTaskInfo(task.GroupId, task.Id)
	if data, err := json.Marshal(task); err == nil {
		if _, err := conn.Set(path, data); err == nil {
			return nil
		} else {
			return err
		}
	} else {
		return err
	}
}

func (task *Task) RsyncTaskFromZk(conn *zookeeper.Zookeeper) {
	log.Info("\033[31m [Rsync Task From Zk]\033[0m  start...")
	//conn := zookeeper.GetZkConn()
	//defer conn.Close()
	path := config.GetTaskInfo(task.GroupId, task.Id)
	if data, _, err := conn.Get(path); err == nil {
		_task := Task{}
		err = json.Unmarshal(data, &_task)
		if err != nil {
			log.Error("Json Decode failed", err)
		}
		task.Sharding = _task.Sharding
		task.ShardingTotalCount = _task.ShardingTotalCount
		task.Description = _task.Description
	} else {
		log.Error("Get Zookeeper Data Failed, zk Path:" + path)
	}
	log.Info("\033[31m [Rsync Task From Zk]\033[0m  end...", task)
}

func (task *Task) GetTaskWorkers(conn *zookeeper.Zookeeper) (children []string, err error) {
	children, _, err = conn.Children(config.GetWorkerInstancesPath(task.GroupId, task.Id))
	return
}

func (task *Task) Bytes() []byte {
	if data, err := json.Marshal(task); err != nil {
		log.Error(err)
	} else {
		return data
	}

	return nil
}

/**
 * 判断是否注册到zk
 */
func (task *Task) HasRegister() bool {
	conn := zookeeper.GetZkConn()
	defer conn.Close()
	if exists, _, err := conn.Exists(config.GetTaskRootPath(task.GroupId, task.Id)); err != nil {
		log.Error("cannot connect zookeeper")
	} else {
		return exists
	}
	return false
}

// 创建永久目录
func (task *Task) CreateTaskOnZk(conn *zookeeper.Zookeeper) {
	// tasks/groupId
	conn.Create(config.GetTaskGroupRootPath(task.GroupId), nil, 0, zk.WorldACL(zk.PermAll))
	// tasks/groupId/taskId
	conn.Create(config.GetTaskRootPath(task.GroupId, task.Id), nil, 0, zk.WorldACL(zk.PermAll))
	// tasks/groupId/info
	conn.Create(config.GetTaskInfo(task.GroupId, task.Id), task.Bytes(), 0, zk.WorldACL(zk.PermAll))
	// tasks/groupId/instances/
	conn.Create(config.GetWorkerInstancesPath(task.GroupId, task.Id), nil, 0, zk.WorldACL(zk.PermAll))
	// tasks/groupId/managers
	conn.Create(config.GetTaskManagerPath(task.GroupId, task.Id), nil, 0, zk.WorldACL(zk.PermAll))
}
