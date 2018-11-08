package manager

import (
	"fmt"
	"github.com/lpflpf/goschedule/common/config"
	"github.com/lpflpf/goschedule/common/tasks"
	"github.com/lpflpf/goschedule/common/utils"
	"github.com/lpflpf/goschedule/common/utils/set"
	"github.com/lpflpf/goschedule/common/work"
	"github.com/lpflpf/goschedule/common/zookeeper"
	"github.com/lpflpf/goschedule/manager/schedule"
	"github.com/qiniu/log"
	"github.com/samuel/go-zookeeper/zk"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strings"
	"sync"
)

type Manager struct {
	conn               *zookeeper.Zookeeper
	Data               *MData
	chEvent            chan Event
	chTaskUnWatchMap   map[string]chan struct{}
	chWorkerUnWatchMap map[string]chan struct{}
}

func NewManager(config *config.ManagerConfig) *Manager {
	manager := &Manager{
		chEvent:            make(chan Event),
		chTaskUnWatchMap:   make(map[string]chan struct{}),
		chWorkerUnWatchMap: make(map[string]chan struct{}),
		conn:               zookeeper.GetZkConn(),
		//InstanceId:
		Data: &MData{
			Status:     MOnline,
			ServerIp:   utils.GetInternalIp(),
			ProcessId:  os.Getpid(),
			RpcPort:    config.RpcHost,
			WatchTasks: []*tasks.Task{},
			InstanceId: "manager:" + strings.Replace(config.RpcHost, "/", "_", -1),
		},
	}

	// 创建软链
	manager.registerOnZk()
	if err := manager.Data.RsyncToZk(); err != nil {
		log.Error(err)
	}

	// 事件处理
	go manager.eventHandling()

	// 监听Manager 的工作
	go manager.monitorManager()

	deal := &MessageDeal{}
	deal.manager = manager
	log.Info("Listen:", config.RpcHost)
	rpc.Register(deal)
	rpc.HandleHTTP()
	port := ":" + strings.Split(config.RpcHost, ":")[1]
	if listen, err := net.Listen("tcp", port); err == nil {
		http.Serve(listen, nil)
	} else {
		log.Error("rpc failed")
	}

	return manager
}

// 在kafka上注册manager 服务
func (manager *Manager) registerOnZk() {
	manager.conn.Create(config.GetManagerInstanceAllPath(manager.Data.InstanceId), nil, 0, zk.WorldACL(zk.PermAll))
	path := config.GetManagerInstance(manager.Data.InstanceId)
	manager.conn.Create(config.GetManagersInstancesPath(), nil, 0, zk.WorldACL(zk.PermAll))
	manager.conn.Create(path, nil, zk.FlagEphemeral, zk.WorldACL(zk.PermAll))
}

func (manager *Manager) addTask(message TaskAddMessage) {
	log.Info("\033[31m[Add Task]\033[0m Starting...", message)
	lock := manager.conn.NewLock(config.GetLockDoNewTaskPath(message.groupId, message.taskId))
	lock.Lock()
	defer lock.Unlock()
	task := tasks.NewTask(message.groupId, message.taskId)
	task.RsyncTaskFromZk(manager.conn)
	if data, err := tasks.GetTaskManagerData(task, manager.conn); err != nil {
		log.Error(fmt.Printf("Get task worker(%s:%s) manager data failed.", message.groupId, message.taskId))
		return
	} else if len(data.Managers) > 0 {
		log.Info(fmt.Printf("Task %s:%s has added", message.groupId, message.taskId))
		return
	}

	managersData := GetOnlineManagers()
	count := len(managersData)
	if count > 5 {
		count = 5
	}

	for i := 0; i < count; i++ {
		key := GetMinTaskNum(managersData)
		log.Info(manager.Data.InstanceId, "\tManager:"+key+" Watch Task (groupId:"+task.GroupId+",taskId:"+task.Id+")")
		if key == manager.Data.InstanceId {
			manager.watchTaskAndReady(task)
			manager.addWorker(task)
			manager.Data.WatchTask(task)
			log.Info("Need worker Add ...")
			continue
		}
		data := managersData[key]

		// send message add
		if client, err := rpc.DialHTTP("tcp", data.RpcPort); err == nil {
			log.Info(manager.Data.InstanceId, "\t dial to ", data.RpcPort)
			var ret int
			err := client.Call("MessageDeal.AddWatch", task, &ret)
			log.Info(err, task, ret)

			if err == nil {
				managersData[key].WatchTasks = append(managersData[key].WatchTasks, task)
				// TODO log
			} else {
				i--
				//TODO log
				log.Error(err)
			}
		} else {
			log.Error(err)
			i--
		}
	}
}

func (manager *Manager) addWorker(task *tasks.Task) {
	log.Info("Add Worker", task.GroupId+"/"+task.Id)

	lock := manager.conn.NewLock("event:update_worker:" + task.GroupId + "|" + task.Id)
	lock.Lock()
	defer lock.Unlock()

	if data, err := manager.getTaskInstances(task); err != nil {
		log.Error("del worker, getTaskInstances Error. ", err)
	} else {
		task.RsyncTaskFromZk(manager.conn)

		distribute := map[string]*work.WData{}
		doingSharding := map[int]struct{}{}
		//needUpdate := map[string]struct{}{}
		needUpdate := set.New()
		emptyWorkNum := 0

		// 当前存在的实例，做任务分配
		for _, child := range data {
			if tmp := GetWData(task, child, manager.conn); tmp != nil {
				distribute[child] = tmp
				for k := range tmp.Sharding {
					doingSharding[k] = struct{}{}
				}

				if len(tmp.Sharding) == 0 {
					emptyWorkNum += 1
				}
			}
		}

		if emptyWorkNum == 0 {
			return
		}

		noSharding := len(task.Sharding) - len(doingSharding)

		log.Info("noSharding ", noSharding)
		if noSharding > 0 {
			for k, val := range task.Sharding {
				if _, ok := doingSharding[k]; !ok {
					min := 999
					cur := ""
					for instanceId, wr := range distribute {
						if wr.Status == work.IdleStatus || wr.Status == work.FailedStatus {
							continue
						}
						if len(wr.Sharding) < min {
							min = len(wr.Sharding)
							cur = instanceId
						}
					}
					distribute[cur].Sharding[k] = val
					needUpdate.Add(cur)
				}
			}
		} else if task.ShardingTotalCount >= len(distribute) {
			size := len(task.Sharding)
			for iter := 0; iter < size; iter++ {
				max := -1
				var maxWr *work.WData
				var maxInstanceId string

				for instanceId, wr := range distribute {
					if len(wr.Sharding) > max {
						max = len(wr.Sharding)
						maxWr = wr
						maxInstanceId = instanceId
					}
				}

				min := max + 1
				var minWr *work.WData
				var minInstanceId string

				for instanceId, wr := range distribute {
					if len(wr.Sharding) < min {
						min = len(wr.Sharding)
						minWr = wr
						minInstanceId = instanceId
					}
				}

				if minInstanceId == maxInstanceId {
					break
				}

				needUpdate.Add(minInstanceId)
				needUpdate.Add(maxInstanceId)

				for k, v := range maxWr.Sharding {
					minWr.Sharding[k] = v
					delete(maxWr.Sharding, k)
					break
				}
			}
		}

		for _, instance := range needUpdate.List() {
			log.Info("update job instance ", task)
			distribute[instance.(string)].Status = work.ReBalanceStatus
			distribute[instance.(string)].Source = manager.Data.InstanceId
			distribute[instance.(string)].RsyncToZk(task, instance.(string), manager.conn)
		}
	}
}

func (manager *Manager) delWorker(message WorkerModifyMessage) {
	log.Info("Del Worker" + message.groupId + "/" + message.taskId)

	// step1  加锁
	// 分配任务
	// 解锁

	lock := manager.conn.NewLock("event:update_worker:" + message.groupId + "|" + message.taskId)
	lock.Lock()
	defer lock.Unlock()

	task := tasks.NewTask(message.groupId, message.taskId)
	if data, err := manager.getTaskInstances(task); err != nil {
		log.Error("del worker, getTaskInstances Error. ", err)
	} else {
		task.RsyncTaskFromZk(manager.conn)

		distribute := make(map[string]*work.WData)

		doingSharding := map[int]struct{}{}

		// 当前存在的实例，做任务分配
		for _, child := range data {
			if tmp := GetWData(task, child, manager.conn); tmp != nil {
				distribute[child] = tmp
				for d := range tmp.Sharding {
					doingSharding[d] = struct{}{}
				}
			}
		}

		// 已经有manager做了处理
		if len(doingSharding) == len(task.Sharding) {
			return
		}

		needUpdate := map[string]struct{}{}

		for k, val := range task.Sharding {
			if _, ok := doingSharding[k]; !ok {
				min := 999
				cur := ""
				for instanceId, wr := range distribute {
					if wr.Status == work.IdleStatus || wr.Status == work.FailedStatus {
						continue
					}
					if len(wr.Sharding) < min {
						min = len(wr.Sharding)
						cur = instanceId
					}
				}
				distribute[cur].Sharding[k] = val
				needUpdate[cur] = struct{}{}
			}
		}

		for instance := range needUpdate {
			distribute[instance].Status = work.ReBalanceStatus
			distribute[instance].Source = manager.Data.InstanceId
			distribute[instance].RsyncToZk(task, instance, manager.conn)
		}
	}
}

/**
 * 增加管理者
 */
func (manager *Manager) addManager(message ManagerModifyMessage) error {
	//conn := zookeeper.GetZkConn()
	//defer conn.Close()
	conn := manager.conn
	if curChildren, err := manager.getManagerInstances(); err != nil {
		return err
	} else {
		addPath := utils.ArraySubtract(curChildren, message.children)
		for _, path := range addPath {
			if strings.HasPrefix(path, "__$$") {

				ids := strings.Split(strings.TrimPrefix(path, "__$$"), "$$")
				taskAddMessage := TaskAddMessage{}
				taskAddMessage.groupId = ids[0]
				taskAddMessage.taskId = ids[1]
				manager.addTask(taskAddMessage)
				log.Info("\033[31m Add Task \033[0m" + "groupId: " + ids[0] + ", jobId: " + ids[1])
				conn.Delete(config.GetMessageTaskOnline(taskAddMessage.groupId, taskAddMessage.taskId))
			} else {
				// continue
				log.Info("\033[31m Add Manager \033[0m" + path)
			}
		}
	}

	return nil
}

// step 1 若 前缀为__$$, 则为task Add 操作完成的消息，不做处理
// step 2 lock path   【若处理下线的manager此时下线，则manager的状态不会更新，其他manager 可以处理之】
// step 3 判断是否已经更新下线manager的状态，若下线manager的状态已经更新为下线，
// step 3 若下线manager的状态未更新，则该manager 处理该机器的下线
// step 4 剔除 manager watch的所有task，如果manager 过少，则获取新的manager watch task
// step 5 更新下线manager 的状态 为下线
// step 5 unlock path
func (manager *Manager) delManager(message ManagerModifyMessage) error {
	var curOnlineMInstances []string
	curOnlineMInstances, err := manager.getManagerInstances()
	log.Info("Del", curOnlineMInstances, message.children)
	if err != nil {
		return err
	}

	delPath := utils.ArraySubtract(message.children, curOnlineMInstances)
	for _, instanceId := range delPath {
		// 若 前缀为__$$, 则为task Add 操作完成的消息，不做处理
		if strings.HasPrefix(instanceId, "__$$") {
			continue
		}
		log.Info("\033[31m Delete Manager \033[0m" + instanceId)
		if instanceId == manager.Data.InstanceId {
			continue
		}

		lock := manager.conn.NewLock("event:del-manager:" + instanceId)
		if err := lock.Lock(); err != nil {
			log.Error("cannot lock del_manager", err)
		}

		managerData, err := GetManagerData(instanceId)
		if err != nil {
			lock.Unlock()
			log.Error("get Manager "+instanceId+" data error!", err)
			return nil
		}

		if managerData.Status == MOffline { // 已经做了下线处理，则忽略
			lock.Unlock()
			log.Info("unlock del_manager: " + instanceId)
			continue
		} else {
			for _, task := range managerData.WatchTasks {
				managers := GetOnlineManagers()
				taskManagerData, err := tasks.GetTaskManagerData(task)
				if err != nil {
					// TODO
				}
				delete(taskManagerData.Managers, instanceId)
				taskManagerData.RsyncToZk(task, manager.conn)

				for i := len(taskManagerData.Managers); i <= 5 && len(managers) != 0; i++ {
					key := GetMinTaskNum(managers)
					if _, ok := taskManagerData.Managers[key]; ok {
						delete(managers, key)
						i--
					} else {
						if key == manager.Data.InstanceId {
							delete(managers, key)
							manager.watchTaskAndReady(task)
							manager.Data.WatchTask(task, manager.conn)
						} else if client, err := rpc.Dial("tcp", managers[key].RpcPort); err == nil {
							var ret int
							err := client.Call("MessageDeal.AddWatch", task, &ret)
							if err == nil {
								delete(managers, key)
								// TODO log
							} else {
								i--
							}
						}
					}
				}
			}
		}

		managerData.Status = MOffline
		managerData.WatchTasks = []*tasks.Task{}
		managerData.RsyncToZk()

		if err := lock.Unlock(); err != nil {
			log.Info(err)
		}
	}
	return nil
}

func (manager *Manager) eventHandling() {
	for {
		select {
		case event := <-manager.chEvent:
			switch event.eventType {
			case TaskAdd:
				manager.addTask(event.message.(TaskAddMessage))
			case WorkerAdd:
				manager.addWorker(tasks.NewTask(event.message.(WorkerModifyMessage).groupId, event.message.(WorkerModifyMessage).taskId))
			case WorkerDel:
				manager.delWorker(event.message.(WorkerModifyMessage))
			case ManagerAdd:
				manager.addManager(event.message.(ManagerModifyMessage))
			case ManagerDel:
				manager.delManager(event.message.(ManagerModifyMessage))
			case TaskWatch:

			}
		}
	}
}

func (manager *Manager) getManagerInstances() ([]string, error) {
	return manager.getChildren(config.GetManagersInstancesPath())
}

func (manager *Manager) getTaskInstances(task *tasks.Task) ([]string, error) {
	return manager.getChildren(config.GetWorkerInstancesPath(task.GroupId, task.Id))
}

func (manager *Manager) getChildren(path string) ([]string, error) {
	if children, _, err := manager.conn.Children(path); err != nil {
		return nil, err
	} else {
		return children, nil
	}
}

/**
 * 监听manager的修改
 */
func (manager *Manager) monitorManager() {
	log.Info("Monitor Manager...")
	for {
		children, _, e, err := manager.conn.ChildrenW(config.GetManagersInstancesPath())
		if err == nil {
			event := <-e
			log.Info(event.Type, children)
			message := ManagerModifyMessage{
				children: children,
				e:        event,
			}

			switch event.Type {
			case zk.EventNodeCreated:
				manager.chEvent <- Event{ManagerAdd, message}
			case zk.EventNodeDeleted:
				manager.chEvent <- Event{ManagerDel, message}
			case zk.EventNodeChildrenChanged:
				if curOnlineMInstances, err := manager.getManagerInstances(); err == nil {
					if len(utils.ArraySubtract(curOnlineMInstances, children)) > 0 {
						manager.chEvent <- Event{ManagerAdd, message}
					}

					if len(utils.ArraySubtract(children, curOnlineMInstances)) > 0 {
						manager.chEvent <- Event{ManagerDel, message}
					}
				} else {
					log.Error(err)
				}
			}
		}
	}
}

// UnWatch 某个Task
func (manager *Manager) UnWatchTask(task *tasks.Task) {
	key := task.GroupId + "$$" + task.Id
	if val, ok := manager.chTaskUnWatchMap[key]; ok && val != nil {
		manager.chTaskUnWatchMap[key] <- struct{}{}
	}
}

func (manager *Manager) watchTaskAndReady(task *tasks.Task) {
	watchOk := make(chan struct{})
	go manager.watchTask(task, watchOk)
	<-watchOk
}

func (manager *Manager) UnWatchWorker(task *tasks.Task, workerId string) {
	key := task.GroupId + "$$" + task.Id + "$$" + workerId
	if val, ok := manager.chWorkerUnWatchMap[key]; ok && val != nil {
		manager.chWorkerUnWatchMap[key] <- struct{}{}
	}
}

func (manager *Manager) WatchWorkerAndReady(task *tasks.Task, workerId string) {
	watchOk := make(chan struct{})
	go manager.watchWorker(task, workerId, watchOk)
	<-watchOk
}

func (manager *Manager) ReBalanceTask(task *tasks.Task) error {
	lock := manager.conn.NewLock("event:update_worker:" + task.GroupId + "|" + task.Id)
	lock.Lock()
	defer lock.Unlock()

	if data, err := manager.getTaskInstances(task); err != nil {
		log.Error("ReBalanceTask, getTaskInstances Error. ", err)
	} else {
		task.RsyncTaskFromZk(manager.conn)
		distribute := map[string]*work.WData{}
		for _, child := range data {
			if tmp := GetWData(task, child, manager.conn); tmp != nil {
				distribute[child] = tmp
			}
		}

		strategy := schedule.GetStrategy("min_update")
		distribute = strategy.Schedule(distribute, task)
		manager.SetWorkersReBalance(distribute, task)
	}

	return nil
}

//TODO 对于关闭较慢的实例，是否需要先做停止操作，再启用？
// TODO reBalance 操作 分解步骤
func (manager *Manager) SetWorkersReBalance(distribute map[string]*work.WData, task *tasks.Task) {
	for instance, wr := range distribute {
		wr.Status = work.ReBalanceStatus
		wr.Source = "manager"
		wr.RsyncToZk(task, instance, manager.conn)
	}
}

func (manager *Manager) watchWorker(task *tasks.Task, workerId string, watchOk chan struct{}) {
	log.Info("[Watch Worker groupId:" + task.GroupId + ", taskId:" + task.Id + ", instanceId:" + workerId + "] Start...")
	workerUniqueKey := task.GroupId + "$$" + task.Id + "$$" + workerId
	manager.chWorkerUnWatchMap[workerUniqueKey] = make(chan struct{})
	defer func() {
		log.Info("[Watch Worker groupId:" + task.GroupId + ", taskId:" + task.Id + ", instanceId:" + workerId + "] End...")
		close(manager.chWorkerUnWatchMap[workerUniqueKey])
		delete(manager.chWorkerUnWatchMap, workerUniqueKey)
	}()

WorkerUnWatch:
	for {
		select {
		case <-manager.chWorkerUnWatchMap[workerUniqueKey]:
			break WorkerUnWatch
		default:
			_, stat, e, err := manager.conn.GetW(config.GetWorkerInstancePath(task.GroupId, task.Id, workerId))
			if err != nil {
				continue
			}
			watchOk <- struct{}{}
			event := <-e
			lock := manager.conn.NewLock(config.GetLockUpdateWDataPath(task.GroupId, task.Id, workerId))
			lock.Lock()

		EventDeal:
			switch event.Type {
			case zk.EventNodeDataChanged:
				data, statN, err := manager.conn.Get(config.GetWorkerInstancePath(task.GroupId, task.Id, workerId))
				if err != nil {
					log.Error("cannot get worker instance data", err)
					break EventDeal
				}

				if stat.Version+1 < statN.Version {
					break EventDeal
				}
				wData := work.NewWData()
				wData.LoadRecordFromBytes(data)

				if wData.Source == "manager" {
					break EventDeal
				}

				switch wData.Status {
				case work.ReadyStatus:
					// reBalance
					manager.ReBalanceTask(task)
					wData.Status = work.PendingStatus
				case work.SuccessStatus:
					// update to ready.
					wData.Status = work.ReadyStatus
				case work.FailedStatus:
					// update to idle.
					wData.Status = work.IdleStatus
				case work.IdleStatus:
					// reBalance
					manager.ReBalanceTask(task)
				case work.RunningStatus:
					break EventDeal
				case work.PendingStatus:
					break EventDeal
				}

				wData.Source = "manager"
				wData.RsyncToZk(task, workerId)
			case zk.EventNodeDeleted:
				break WorkerUnWatch
			}

			lock.Unlock()
		}
	}
	// TODO getW(workerData)
}

// watch 某个Task
func (manager *Manager) watchTask(task *tasks.Task, watchOk chan struct{}) {
	log.Info("[Watch Task groupId:" + task.GroupId + ", taskId:" + task.Id + "] Start...")
	taskUniqueKey := task.GroupId + "$$" + task.Id
	manager.chTaskUnWatchMap[taskUniqueKey] = make(chan struct{})
	defer func() {
		log.Info("[Watch Task groupId:" + task.GroupId + ", taskId:" + task.Id + "] End...")
		close(manager.chTaskUnWatchMap[taskUniqueKey])
		delete(manager.chTaskUnWatchMap, taskUniqueKey)
	}()

	conn := manager.conn

	instancesPath := config.GetWorkerInstancesPath(task.GroupId, task.Id)
	o := &sync.Once{}
TaskUnWatch:
	for {
		select {
		case <-manager.chTaskUnWatchMap[taskUniqueKey]:
			break TaskUnWatch
		default:
			children, _, e, err := conn.ChildrenW(instancesPath)
			if err == nil {
				o.Do(func() {
					watchOk <- struct{}{}
					close(watchOk)
				})
				event := <-e
				var eventType EventType
				switch event.Type {
				case zk.EventNodeChildrenChanged:
					if data, err := manager.getTaskInstances(task); err == nil {
						if len(utils.ArraySubtract(data, children)) > 0 {
							manager.chEvent <- Event{
								eventType: WorkerAdd,
								message:   WorkerModifyMessage{children, <-e, task.GroupId, task.Id},
							}
						} else if len(utils.ArraySubtract(children, data)) > 0 {
							manager.chEvent <- Event{
								eventType: WorkerDel,
								message:   WorkerModifyMessage{children, <-e, task.GroupId, task.Id},
							}
						}
					}
					continue
				case zk.EventNodeDeleted:
					eventType = WorkerDel
				case zk.EventNodeCreated:
					eventType = WorkerAdd
				}
				manager.chEvent <- Event{
					eventType: eventType,
					message:   WorkerModifyMessage{children, <-e, task.GroupId, task.Id},
				}
			} else {
				log.Error(err)
			}
		}
	}
}

// 将该Manager 添加到某个task 中，修改task的 manager 数据
func (manager *Manager) AppendManager2Task(task *tasks.Task) {
	conn := manager.conn

	lock := conn.NewLock("data:" + config.GetLockTaskManagerData(task.GroupId, task.Id))
	lock.Lock()
	defer lock.Unlock()

	if data, err := tasks.GetTaskManagerData(task, conn); err == nil {
		if _, ok := data.Managers[manager.Data.InstanceId]; !ok {
			data.Managers[manager.Data.InstanceId] = struct{}{}
			data.RsyncToZk(task, conn)
		}
	}
}
