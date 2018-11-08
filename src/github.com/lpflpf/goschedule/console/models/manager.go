package models

import (
	"github.com/lpflpf/goschedule/common/config"
	"github.com/lpflpf/goschedule/common/zookeeper"
	"github.com/lpflpf/goschedule/manager/manager"
)

func GetAllManagers() map[string]*manager.MData {
	conn := zookeeper.GetZkConn()
	var children []string
	var err error
	children, _, err = conn.Children(config.GetManagerAllPath())
	if err != nil {
		return nil
	}

	mInfo := map[string]*manager.MData{}
	for _, instanceId := range children {
		var mData *manager.MData
		mData, _ = manager.GetManagerData(instanceId, conn)
		mInfo[instanceId] = mData
	}

	return mInfo
}
