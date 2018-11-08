package schedule

import (
	"github.com/lpflpf/goschedule/common/work"
	"github.com/lpflpf/goschedule/manager/schedule/impl"
	"github.com/lpflpf/goschedule/common/tasks"
)

type Strategy interface {
	Schedule(map[string]*work.WData, *tasks.Task) map[string]*work.WData
}

func GetStrategy(strategyType string) Strategy {
	var strategy Strategy
	switch strategyType {
	case "min_update":
		strategy = impl.MinUpdateStrategy{}
		return strategy
	}
	return nil
}
