package manager

import "github.com/samuel/go-zookeeper/zk"

type EventType int

const (
	_ EventType = iota
	TaskAdd
	WorkerAdd
	WorkerDel
	ManagerAdd
	ManagerDel
	TaskWatch
)

type Event struct {
	eventType EventType
	message   interface{}
}

type CommonMessage struct {
	children []string
	e        zk.Event
	groupId  string
	taskId   string
}

type TaskAddMessage struct {
	groupId  string
	taskId   string
}

type WorkerModifyMessage struct {
	children []string
	e        zk.Event
	groupId  string
	taskId   string
}

type ManagerModifyMessage struct {
	children []string
	e        zk.Event
}

type WatchTaskMessage struct {

}