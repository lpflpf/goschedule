package actions

import "net/http"

func Start() {
	mapper := map[string]func(w http.ResponseWriter, r *http.Request){
		"/manager":       GetAllManagers,
		"/task/info":     GetTaskInfo,
		"/task/managers": GetTaskManagers,
		"/task/workers":  GetTaskWorkers,
	}

	for pattern, call := range mapper {
		http.HandleFunc(pattern, call)
	}

	http.ListenAndServe(":8000", nil)
}
