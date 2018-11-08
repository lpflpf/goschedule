package actions

import (
	"github.com/lpflpf/goschedule/common/tasks"
	"net/http"
	"github.com/lpflpf/goschedule/console/models"
)

func GetTaskInfo(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()
	task := tasks.NewTask(r.Form["groupId"][0], r.Form["taskId"][0])
	outJson(w, models.GetTaskInfo(task))
}

func GetTaskManagers(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()
	task := tasks.NewTask(r.Form["groupId"][0], r.Form["taskId"][0])
	outJson(w, models.GetTaskManagers(task))
}

func GetTaskWorkers(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()
	task := tasks.NewTask(r.Form["groupId"][0], r.Form["taskId"][0])
	outJson(w, models.GetTaskWorkers(task))
}
