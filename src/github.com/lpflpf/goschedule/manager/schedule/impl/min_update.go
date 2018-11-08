package impl

import (
	"github.com/lpflpf/goschedule/common/tasks"
	"github.com/lpflpf/goschedule/common/work"
	"sort"
)

type MinUpdateStrategy struct{}

type wRData struct {
	InstanceId string
	NeedUpdate bool
	*work.WData
}

type wRDataList struct {
	averageShardingCount int
	data                 []*wRData
}

func (list wRDataList) Len() int {
	return len(list.data)
}

func (list wRDataList) Less(i, j int) bool {
	iLen := len(list.data[i].Sharding)
	jLen := len(list.data[j].Sharding)

	if iLen == list.averageShardingCount {
		return false
	}
	if jLen == list.averageShardingCount {
		return true
	}

	if iLen == list.averageShardingCount+1 {
		return false
	}

	if jLen == list.averageShardingCount+1 {
		return true
	}

	return iLen < jLen
}

func (list wRDataList) Swap(i, j int) {
	list.data[i], list.data[j] = list.data[j], list.data[i]
}

func (list wRDataList) append(instanceId string, wdata *work.WData) {
	list.data = append(list.data, &wRData{instanceId, false, wdata})
}

func (list wRDataList) max() int {
	if list.Len() == 0 {
		return -1
	}

	maxKey := 0
	maxCount := len(list.data[0].Sharding)

	for i := 1; i < list.Len(); i++ {
		if maxCount > len(list.data[i].Sharding) {
			maxCount = len(list.data[i].Sharding)
			maxKey = i
		}
	}

	return maxKey
}

func (list wRDataList) min() int {
	if list.Len() == 0 {
		return -1
	}
	minKey := 0
	minCount := len(list.data[0].Sharding)
	for i := 1; i < list.Len(); i++ {
		if minCount > len(list.data[i].Sharding) {
			minCount = len(list.data[i].Sharding)
			minKey = i
		}
	}

	return minKey
}

func (list wRDataList) getItemSize(key int) int {
	if len(list.data) <= key {
		return -1
	}
	return len(list.data[key].Sharding)
}

func (list wRDataList) appendItemSharding(key int, shardingKey int, sharding string) {
	list.data[key].Sharding[key] = sharding
}

func (list wRDataList) needUpdate(key int) {
	list.data[key].NeedUpdate = true
}

func (list wRDataList) popItemSharding(key int) (int, string) {
	for key, val := range list.data[key].Sharding {
		delete(list.data[key].Sharding, key)
		return key, val
	}

	key, val := popMap(list.data[key].Sharding)
	delete(list.data[key].Sharding, key)
	return key, val
}

func (list wRDataList) getNeedUpdateWorker() map[string]*work.WData {
	data := map[string]*work.WData{}
	for _, workerData := range list.data {
		if workerData.NeedUpdate {
			data[workerData.InstanceId] = workerData.WData
		}
	}

	return data
}

func popMap(mapData map[int]string) (int, string) {
	for key, val := range mapData {
		return key, val
	}

	return -1, ""
}

func (schedule MinUpdateStrategy) Schedule(data map[string]*work.WData, task *tasks.Task) map[string]*work.WData {
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
	list.averageShardingCount = len(task.Sharding) / list.Len()
	sort.Sort(list)

	for {
		minKey := list.min()
		if list.getItemSize(minKey) == list.averageShardingCount {
			break
		}

		list.needUpdate(minKey)
		if len(noSharding) != 0 {
			key, val := popMap(noSharding)
			delete(noSharding, key)
			list.appendItemSharding(minKey, key, val)
		} else {
			maxKey := list.max()
			key, val := list.popItemSharding(maxKey)
			list.appendItemSharding(minKey, key, val)
			list.needUpdate(maxKey)
		}
	}

	return list.getNeedUpdateWorker()
}
