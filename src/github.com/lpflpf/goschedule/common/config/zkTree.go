package config

import (
	"fmt"
)

const (
	tasksPath    = "/tasks"
	managersPath = "/managers"
)

func GetTaskGroupRootPath(groupId string) string {
	return tasksPath + "/" + groupId
}

// /tasks/groupId/taskId
func GetTaskRootPath(groupId, taskId string) string {
	return fmt.Sprintf("%s/%s/%s", tasksPath, groupId, taskId)
}

// /tasks/groupId/taskId/instances
func GetWorkerInstancesPath(groupId, taskId string) string {
	return fmt.Sprintf("%s/%s", GetTaskRootPath(groupId, taskId), "instances")
}

// /tasks/groupId/taskId/instances/instanceId
func GetWorkerInstancePath(groupId, taskId, instanceId string) string {
	return fmt.Sprintf("%s/%s", GetWorkerInstancesPath(groupId, taskId), instanceId)
}

// /tasks/groupId/taskId/managers
func GetTaskManagerPath(groupId, taskId string) string {
	return fmt.Sprintf("%s/%s", GetTaskRootPath(groupId, taskId), "managers")
}

// /tasks/groupId/taskId/info
func GetTaskInfo(groupId, taskId string) string {
	return fmt.Sprintf("%s/%s", GetTaskRootPath(groupId, taskId), "info")
}

// /groupid$$taskId$$create_task
func GetLockNewTaskPath(groupId, taskId string) string {
	return fmt.Sprintf("%s$$%s$$create_task", groupId, taskId)
}

// /groupid$$taskId$$update_status
func GetLockUpdateTaskStatusPath(groupId, taskId string) string {
	return fmt.Sprintf("%s$$%s$$update_status", groupId, taskId)
}

func GetLockUpdateWDataPath(groupId, taskId, instanceId string) string {
	return fmt.Sprintf("%s$$%s$$%s$$update_status", groupId, taskId)
}

func GetLockTaskManagerData(groupId, taskId string) string {
	return fmt.Sprintf("%s$$%s$$manager_data", groupId, taskId)
}

// manager$$instanceId
func GetLockManagerData(instanceId string) string {
	return "/manager$$" + instanceId
}

// /groupid$$taskId$$do_create_task
func GetLockDoNewTaskPath(groupId, taskId string) string {
	return fmt.Sprintf("%s$$%s$$do_create_task", groupId, taskId)
}

// /managers/instances/
func GetManagerInstance(instanceId string) string {
	return fmt.Sprintf("%s/instances/%s", managersPath, instanceId)
}

// /managers/instances/__$$
func GetMessageTaskOnline(groupId, taskId string) string {
	return fmt.Sprintf("%s/instances/__$$%s$$%s", managersPath, groupId, taskId)
}

// /managers/intances
func GetManagersInstancesPath() string {
	return managersPath + "/instances"
}

func GetManagerAllPath() string {
	return fmt.Sprintf("%s/all", managersPath)
}

func GetManagerInstanceAllPath(instanceId string) string {
	return fmt.Sprintf("%s/all/%s", managersPath, instanceId)
}
