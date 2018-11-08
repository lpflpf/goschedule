package worker

import (
	"fmt"
	"github.com/lpflpf/goschedule/common/config"
	"github.com/lpflpf/goschedule/common/tasks"
	"github.com/lpflpf/goschedule/common/zookeeper"
	"github.com/qiniu/log"
	"github.com/samuel/go-zookeeper/zk"
	"github.com/satori/go.uuid"
	"sync"
	"github.com/lpflpf/goschedule/common/work"
)

type Worker struct {
	instanceZkPath        string
	instanceId            string
	WData                 *work.WData
	CallbackUpdateJobInfo func()
	ChStart               chan work.WData // 开始任务
	ChStop                chan work.WData // 停止任务
	ChReBalance           chan work.WData
	ChFinish              chan work.WData // 任务结束
	ChRunning             chan work.WData
	Task                  *tasks.Task
	conn                  *zookeeper.Zookeeper
}

func NewWorker(task *tasks.Task) *Worker {
	worker := &Worker{Task: task}
	if uid, err := uuid.NewV4(); err == nil {
		worker.instanceId = uid.String()
	} else {
		log.Error(fmt.Sprintf("cannot create UUID for Task %s:%s", task.GroupId, task.Id))
	}

	worker.ChStart = make(chan work.WData)
	worker.ChStop = make(chan work.WData)
	worker.ChReBalance = make(chan work.WData)
	worker.ChFinish = make(chan work.WData)
	worker.WData = work.NewWData()
	worker.instanceZkPath = config.GetWorkerInstancePath(task.GroupId, task.Id, worker.instanceId)
	worker.conn = zookeeper.GetZkConn()
	log.Info(worker.instanceZkPath)
	worker.start()
	return worker
}

func (worker *Worker) start() {
	task := worker.Task
	if task.HasRegister() {
		task.RsyncTaskFromZk(worker.conn)
		worker.registerWorkerToZookeeper()
	} else {
		lock := worker.conn.NewLock(config.GetLockNewTaskPath(task.GroupId, task.Id))
		lock.Lock()
		if !task.HasRegister() {
			task.CreateTaskOnZk(worker.conn)
			task.RsyncTaskToZk(worker.conn)
			worker.registerWorkerToZookeeper()
			worker.remindManagerJobOnline()
			lock.Unlock()
		} else {
			lock.Unlock()
			task.RsyncTaskFromZk(worker.conn)
			worker.registerWorkerToZookeeper()
		}
	}
}

func (worker *Worker) LoadWorkerRecord() {
	data, _, err := worker.conn.Get(worker.instanceZkPath)

	if err == nil {
		worker.WData.LoadRecordFromBytes(data)
	}
}

/**
 * 更新worker 的状态，此处需要加锁操作，避免同时并发更新， sharding 只有manager 有权利修改
 */
func (worker *Worker) updateWorkerStatus(status work.WorkerStatus, tip string) {
	lock := worker.conn.NewLock(config.GetLockUpdateTaskStatusPath(worker.Task.GroupId, worker.Task.Id))
	lock.Lock()
	defer lock.Unlock()
	if data, _, err := worker.conn.Get(worker.instanceZkPath); err != nil {
		worker.WData.Status = status
		worker.WData.Message = tip
		worker.WData.LoadRecordFromBytes(data)
		worker.conn.Set(worker.instanceZkPath, worker.WData.Bytes())
	} else {
		// TODO
	}
}

func (worker *Worker) registerWorkerToZookeeper() {
	worker.conn.Create(worker.instanceZkPath, worker.WData.Bytes(), zk.FlagEphemeral, zk.WorldACL(zk.PermAll))
	ready := make(chan struct{})
	go worker.watchWorkerStatus(ready)
	<-ready
}

func (worker *Worker) watchWorkerStatus(ready chan struct{}) {

	conn := zookeeper.GetZkConn()
	once := sync.Once{}
	defer conn.Close()
	for {
		_, _, e, err := conn.GetW(worker.instanceZkPath)
		once.Do(func() {
			ready <- struct{}{}
			close(ready)
		})

		event := <-e
		if err == nil {
			lock := conn.NewLock(config.GetLockUpdateWDataPath(worker.Task.GroupId, worker.Task.Id, worker.instanceId))
			lock.Lock()
			data, _, _ := conn.Get(worker.instanceZkPath)
			worker.WData.LoadRecordFromBytes(data)
			if worker.WData.Source == "worker" {
				lock.Unlock()
				continue
			}
			if event.Type == zk.EventNodeDataChanged {
				switch worker.WData.Status {
				case work.StartStatus:
					worker.ChStart <- *worker.WData
				case work.ReBalanceStatus:
					worker.ChReBalance <- *worker.WData
				case work.StopStatus:
					worker.ChStop <- *worker.WData
				case work.IdleStatus:
					worker.ChStop <- *worker.WData
					worker.WData.Message = "Into idle status."
				}
			}
			worker.WData.Source = "worker"
			worker.WData.Status = work.PendingStatus
			worker.WData.RsyncToZk(worker.Task, worker.instanceId)
			lock.Unlock()
		} else {
			log.Error(fmt.Sprintf("Task %s:%s, zookeeper getW failed", worker.Task.GroupId, worker.Task.Id))
		}
	}
}

func (worker *Worker) listenerChannel() {
	for {
		select {
		case message := <-worker.ChFinish:
			if message.Status == work.SuccessStatus {
				log.Info("worker success finish!")
				worker.updateWorkerStatus(message.Status, "worker finish success!")
			} else if message.Status == work.FailedStatus {
				log.Error("worker do failed."+message.Message, "worker do failed!")
			} else {
				// 不应该出现该情况
			}
		case message := <-worker.ChRunning:
			if message.Status != work.RunningStatus {
				log.Error("channel and status identical, please check your code.")
			}
			worker.updateWorkerStatus(work.RunningStatus, "worker running")
		case <-worker.ChReBalance:
			log.Info("worker reBalance")
		}
	}
}

func (worker *Worker) remindManagerJobOnline() {
	worker.conn.Create(config.GetMessageTaskOnline(worker.Task.GroupId, worker.Task.Id), worker.Task.Bytes(), 0, zk.WorldACL(zk.PermAll))
}
