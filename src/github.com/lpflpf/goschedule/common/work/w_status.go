package work

type WorkerStatus int

//   |     -     | Manager | Worker | Console |    Result     |
//   |   Start   |   √    |   -    |   √    |   source  Worker, Running / FailedStatus |
//   |   Stop    |   √    |   -    |   √    |
//   | ReBalance |   √    |   -    |   √    |
//   | Readying  |    -    |   √   |    -    |
//   | Running   |    -    |   √   |    -    |
//   |   Failed  |    -    |   √   |    -    |
//   | Success   |    -    |   √   |    -    |
//   |  Idle     |    √   |   -    |    -    |

const (
	_ WorkerStatus = iota
	ReadyStatus
	StartStatus
	RunningStatus
	ReBalanceStatus
	StopStatus
	FailedStatus
	SuccessStatus
	IdleStatus  // manager/console 主动停止的worker，进入Idle状态，只有 MC主动唤醒，才可以再次运行
	PendingStatus
)
