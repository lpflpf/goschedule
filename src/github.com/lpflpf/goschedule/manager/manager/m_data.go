package manager

import (
	"encoding/json"
	"github.com/lpflpf/goschedule/common/zookeeper"
	"github.com/lpflpf/goschedule/common/config"
	"github.com/lpflpf/goschedule/common/tasks"
	"github.com/qiniu/log"
)

// /managers/all/instanceId 中存放的数据
type MData struct {
	Status     MStatus
	ServerIp   string
	ProcessId  int
	RpcPort    string
	WatchTasks []*tasks.Task
	InstanceId string
}

func (manager *MData) WatchTask(task *tasks.Task, args ...*zookeeper.Zookeeper) error {
	var conn *zookeeper.Zookeeper
	if len(args) == 0 {
		conn = zookeeper.GetZkConn()
		defer conn.Close()
	} else {
		conn = args[0]
	}

	lock := conn.NewLock("data:manager_data_" + manager.InstanceId)
	lock.Lock()
	defer lock.Unlock()
	for _, _task := range manager.WatchTasks {
		if _task.Id == task.Id || _task.GroupId == task.GroupId {
			return nil
		}
	}

	manager.WatchTasks = append(manager.WatchTasks, task)
	log.Info("add task: ", task, " into ", manager.InstanceId)
	return manager.RsyncToZk()
}

func (manager *MData) RsyncToZk(args ...*zookeeper.Zookeeper) error {
	var conn *zookeeper.Zookeeper
	if len(args) == 0 {
		conn = zookeeper.GetZkConn()
		defer conn.Close()
	} else {
		conn = args[0]
	}
	data, err := json.Marshal(manager)

	if err != nil {
		return err
	}

	if _, err := conn.Set(config.GetManagerInstanceAllPath(manager.InstanceId), data); err != nil {
		return err
	}
	return nil
}

func GetManagerData(instanceId string, args ...*zookeeper.Zookeeper) (*MData, error) {
	var conn *zookeeper.Zookeeper
	if len(args) == 0 {
		conn = zookeeper.GetZkConn()
		defer conn.Close()
	} else {
		conn = args[0]
	}

	path := config.GetManagerInstanceAllPath(instanceId)
	if bData, _, err := conn.Get(path); err == nil {
		data := &MData{}
		err := json.Unmarshal(bData, data)
		if err != nil {
			return nil, err
		}
		return data, nil
	} else {
		return nil, err
	}
}

func GetOnlineManagers() map[string]*MData {

	log.Info("\033[31m[Get Online Managers]\033[0m start... ")

	conn := zookeeper.GetZkConn()
	defer conn.Close()

	path := config.GetManagersInstancesPath()

	managersData := map[string]*MData{}
	if children, _, err := conn.Children(path); err == nil {
		for _, instanceId := range children {
			if data, err := GetManagerData(instanceId); err == nil {
				managersData[instanceId] = data
			}
		}
	}

	log.Info("\033[31m[Get Online Managers]\033[0m return... ", managersData)
	return managersData
}

func GetMinTaskNum(managersData map[string]*MData) string {
	var minInstance string
	minNum := 1000

	for key, val := range managersData {
		if len(val.WatchTasks) < minNum {
			minInstance = key
			minNum = len(val.WatchTasks)
		}
	}
	return minInstance
}
