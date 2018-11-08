package actions

import (
	"net/http"
	"github.com/lpflpf/goschedule/console/models"
)

func GetAllManagers(w http.ResponseWriter, _ *http.Request) {
	outJson(w, models.GetAllManagers())
}
