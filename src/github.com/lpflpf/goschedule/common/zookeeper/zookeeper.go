package zookeeper

import (
	"errors"
	"github.com/lpflpf/goschedule/common/config"
	"github.com/qiniu/log"
	"github.com/samuel/go-zookeeper/zk"
	"strings"
	"time"
)

type Zookeeper struct {
	Prefix string
	Conn   *zk.Conn
	Lock   *zk.Lock
}

var conf *config.ZookeeperConfig

func SetZkConfig(zookeeperConfig *config.ZookeeperConfig) {
	conf = zookeeperConfig
	conn := GetZkConn()
	conn.Create("", nil, 0, zk.WorldACL(zk.PermAll))
	conn.Create("/instances", nil, 0, zk.WorldACL(zk.PermAll))
	conn.Create("/tasks", nil, 0, zk.WorldACL(zk.PermAll))
	conn.Create("/managers", nil, 0, zk.WorldACL(zk.PermAll))
	conn.Create("/managers/all", nil, 0, zk.WorldACL(zk.PermAll))
}

func GetZkConn() *Zookeeper {
	zookeeper := &Zookeeper{
		Prefix: conf.PathPrefix,
	}

	zookeeper.connect()
	zookeeper.Conn.SetLogger(log.Std)
	return zookeeper
}

func (Zk *Zookeeper) Close() {
	Zk.Conn.Close()
}

func (Zk *Zookeeper) connect() {
	if conf == nil {
		log.Fatal("not Register zookeeper config")
	}

	if conn, _, err := zk.Connect(conf.Host, time.Duration(conf.Timeout)*time.Second); err == nil {
		Zk.Conn = conn
	} else {
		log.Fatal("cannot connect Zk")
	}
}
func (Zk *Zookeeper) Exists(pathName string) (bool, *zk.Stat, error) {
	log.Info("Exists", Zk.Prefix+pathName)
	return Zk.Conn.Exists(Zk.Prefix + pathName)
}

func (Zk *Zookeeper) ExistsW(path string) (bool, *zk.Stat, <-chan zk.Event, error) {
	return Zk.Conn.ExistsW(Zk.Prefix + path)
}

func (Zk *Zookeeper) Children(path string) ([]string, *zk.Stat, error) {
	log.Info("get children for path:" + path)
	return Zk.Conn.Children(Zk.Prefix + path)
}

func (Zk *Zookeeper) ChildrenW(path string) ([]string, *zk.Stat, <-chan zk.Event, error) {
	log.Info("watch child path " + path)
	return Zk.Conn.ChildrenW(Zk.Prefix + path)
}
func (Zk *Zookeeper) Create(path string, data []byte, flags int32, acl []zk.ACL) (string, error) {
	log.Info("create for path:" + Zk.Prefix + path)
	return Zk.Conn.Create(Zk.Prefix+path, data, flags, acl)
}

func (Zk *Zookeeper) CreateRecursive(path string, data []byte) error {
	path = Zk.Prefix + path
	token := strings.Split(path, "/")
	curPath := ""
	for _, file := range token {
		curPath = curPath + "/" + file
		if isExist, _, err := Zk.Conn.Exists(curPath); err == nil {
			if isExist {
				continue
			} else {
				if _, err := Zk.Conn.Create(curPath, nil, 0, zk.WorldACL(zk.PermAll)); err != nil {
					return nil
				} else {
					return err
				}
			}
		} else {
			return err
		}
	}

	return nil
}

func (Zk *Zookeeper) Delete(path string) error {
	if _, stat, err := Zk.Conn.Get(Zk.Prefix + path); err == nil {
		return Zk.Conn.Delete(Zk.Prefix+path, stat.Version)
	} else {
		return err
	}
}

func (Zk *Zookeeper) Get(path string) ([]byte, *zk.Stat, error) {
	log.Info("zk-get: " + Zk.Prefix + path)
	return Zk.Conn.Get(Zk.Prefix + path)
}

func (Zk *Zookeeper) Set(path string, data []byte) (*zk.Stat, error) {
	log.Info("set:" + Zk.Prefix + path + "\t data: " + string(data))
	if _, stat, err := Zk.Conn.Get(Zk.Prefix + path); err == nil {
		return Zk.Conn.Set(Zk.Prefix+path, data, stat.Version)
	}

	return nil, errors.New("get data failed")
}

func (Zk *Zookeeper) GetW(path string) ([]byte, *zk.Stat, <-chan zk.Event, error) {
	return Zk.Conn.GetW(Zk.Prefix + path)
}

func (Zk *Zookeeper) NewLock(path string) *zk.Lock {
	log.Info("*****Lock*****" + "\033[31m New Lock \033[0m " + Zk.Prefix + "/locks/" + path)
	return zk.NewLock(Zk.Conn, Zk.Prefix+"/locks/"+path, zk.WorldACL(zk.PermAll))
}
