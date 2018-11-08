package work

import (
	"encoding/json"
	"github.com/lpflpf/goschedule/common/config"
	"github.com/lpflpf/goschedule/common/utils"
	"github.com/lpflpf/goschedule/common/zookeeper"
	"github.com/qiniu/log"
	"os"
	"github.com/lpflpf/goschedule/common/tasks"
)

type WData struct {
	Status    WorkerStatus
	Sharding  map[int]string
	Message   string
	Source    string
	ServerIp  string
	ProcessId int
}


func NewWData() *WData {
	wData := &WData{}
	wData.ProcessId = os.Getpid()
	wData.ServerIp = utils.GetInternalIp()
	wData.Sharding = make(map[int]string)
	wData.Status = ReadyStatus
	return wData
}

func (wData *WData) Bytes() []byte {
	data, err := json.Marshal(wData)
	if err != nil {
		log.Error("Json marshal failed, ", wData)
	}
	log.Info("wData: ", data)
	return data
}

func (wData *WData) UpdateWorkerStatus(task *tasks.Task, wInstanceId string, status WorkerStatus, conn *zookeeper.Zookeeper) error {
	lock := conn.NewLock(config.GetLockUpdateWDataPath(task.GroupId, task.Id, wInstanceId))
	lock.Lock()
	defer lock.Unlock()
	data, _, err := conn.Get(config.GetWorkerInstancePath(task.GroupId, task.Id, wInstanceId))
	if err != nil {
		return err
	}
	wData.LoadRecordFromBytes(data)
	wData.Status = status
	wData.RsyncToZk(task, wInstanceId)
	return nil
}

func (wData *WData) LoadRecordFromBytes(data []byte) {
	tmp := WData{}
	if err := json.Unmarshal(data, &tmp); err != nil {
		log.Error("Json Unmarshal failed, ", data)
	}
	log.Info("update worker record: ", tmp, wData)
	wData.Status = tmp.Status
	wData.Sharding = tmp.Sharding
	wData.Message = tmp.Message
}

func (wData *WData) RsyncToZk(task *tasks.Task, instanceId string, args ...*zookeeper.Zookeeper) (err error) {
	var conn *zookeeper.Zookeeper
	if len(args) == 0 {
		conn = zookeeper.GetZkConn()
		defer conn.Close()
	} else {
		conn = args[0]
	}
	_, err = conn.Set(config.GetWorkerInstancePath(task.GroupId, task.Id, instanceId), wData.Bytes())
	return
}
