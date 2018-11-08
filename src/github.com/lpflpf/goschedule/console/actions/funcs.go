package actions

import (
	"net/http"
	"encoding/json"
)

func outJson(w http.ResponseWriter, data interface{}) {
	bytes, err := json.Marshal(data)
	if err == nil {
		w.Write(bytes)
	}
}
