package worker

type JobConfig struct {
	Id                 string
	GroupId            string
	ShardingTotalCount int
	Sharding           map[int]string
	Description        string
}
