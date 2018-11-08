package config

type ZookeeperConfig struct {
	Host       []string
	Timeout    int
	PathPrefix string
}
