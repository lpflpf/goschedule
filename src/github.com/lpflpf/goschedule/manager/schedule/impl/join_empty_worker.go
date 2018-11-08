package impl

import (
	"github.com/lpflpf/goschedule/common/work"
	"github.com/lpflpf/goschedule/common/tasks"
)

type JoinEmptyWorker struct{}

func (schedule JoinEmptyWorker) Schedule(data map[string]*work.WData, task *tasks.Task) map[string]*work.WData {
	if len(data) == 0 {
		return data
	}
	list := wRDataList{}
	list.data = []*wRData{}

	noSharding := task.Sharding

	for instanceId, wData := range data {
		if wData.Status == work.IdleStatus || wData.Status == work.FailedStatus {
			continue
		}
		for key := range wData.Sharding {
			delete(noSharding, key)
		}

		list.append(instanceId, wData)
	}



	return nil
}
