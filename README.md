## 作业调度器 Golang的一种实现

解决的痛点：
1. 单点问题   实现故障转移
2. 任务调度   可提供的服务增加了，如何动态做任务迁移，而无需重启服务
3. 负载均衡   保持服务的负载均衡，使服务达到最优效果
------

### 设计说明
该调度器底层采用zookeeper，提供Console、Manager、Worker 三个模块。

Console模块：  
  提供一系列操作接口，用于查询Worker、Manager状态，手动调度Manager、Worker进程。

Manager模块：  
  调度工具，用于监控worker的增加或减少，并提供worker调度的策略
 
Worker模块：  
  调度的任务，需要结合worker，并响应worker相关的事件。
  
