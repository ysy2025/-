6.部署模式

本章节,讲解各个部署模式的差异和部署的容错
spark目前支持:local,local-cluster,standalone,第三方部署模式

Driver,应用驱动程序,老板的客户
Master,spark的主控制节点,集群的老板
Worker,Spark的工作节点,集群的主管
Executor,Spark的工作进程,由Worker监管,负责具体任务的执行,相当于打工仔

6.1 local 部署模式
local 部署模式,只有Driver,没有Master和Worker,执行任务的Executor和Driver在同一个JVM中.

local模式下,executorbackend,执行器后端,的实现类,是LocalBackend,有任务需要提交时,由TaskSchedulerImpl,调用LocalBackend的reviveOffers方法申请资源
LocalBackend,向LocalActor发送ReviveOffers消息,申请资源
LocalActor,收到ReviveOffers消息,调用TaskSchedulerImpl的resourceOffers方法申请资源,TaskSchedulerImpl根据条件分配资源
任务获得资源后,调用Executor的launchTask方法运行任务
任务运行过程中,Executor中运行的TaskRunner,通过调用LocalBackend的statusupdate方法,向LocalActor发送statusupdate,更新状态.
任务的状态有,launching,running,finished,failed,killed,lost

6.2 local-cluster 部署模式
local-cluster,伪集群, Driver,Master,Worker,在同一个JVM进程中;可以有多个Worker,每个worker会有多个executor,但是这些executor都独自存在于一个jvm中
和local的其他区别:
使用localsparkcluster启动集群;
sparkdeployschedulerbackend的启动过程不同
appclient的启动和调度
local-cluster模式的任务执行

local-culster[2,1,1024],那么创建TaskSchedulerImpl时,就会匹配local-cluster模式;local-culster[2,1,1024]中,worker为2,worker占用的cpu为1,1024是每个worker指定的内存大小
memoryperslave必须比executormemory大;

local-cluster,除了 由TaskSchedulerImpl 之外,还创建了LocalSparkCluster; LocalSparkCluster的start方法,用来启动集群
local-cluster 模式中,使用的ExecutorBackend,实现类是 sparkdeployschedulerbackend

SparkContext.scala中,createTaskScheduler方法,里面的master,根据不同情况,有不同处理方式
{case LOCAL_CLUSTER_REGEX(numSlaves, coresPerSlave, memoryPerSlave) =>
  // Check to make sure memory requested <= memoryPerSlave. Otherwise Spark will just hang.
  val memoryPerSlaveInt = memoryPerSlave.toInt
  if (sc.executorMemory > memoryPerSlaveInt) {
    throw new SparkException(
      "Asked to launch cluster with %d MB RAM / worker but requested %d MB/worker".format(
        memoryPerSlaveInt, sc.executorMemory))
  }
  val scheduler = new TaskSchedulerImpl(sc)
  val localCluster = new LocalSparkCluster(
    numSlaves.toInt, coresPerSlave.toInt, memoryPerSlaveInt, sc.conf)
  val masterUrls = localCluster.start()
  val backend = new StandaloneSchedulerBackend(scheduler, sc, masterUrls)
  scheduler.initialize(backend)
  backend.shutdownCallback = (backend: StandaloneSchedulerBackend) => {
    localCluster.stop()
  }
  (backend, scheduler)}

首先check,确保 申请的memory<每个slave的memory
如果, sc.executorMemory > memoryPerSlaveInt,报错
反之,
初始化 scheduler,localCluster,masterUrls,backend
最后输出 (backend, scheduler)

6.2.1 LocalSparkCluster的启动
masterActorSystems:用于缓存所有的Master的ActorSystem;
workerActorSystems:维护所有的worker的actorsystem
LocalSparkCluster的start方法用来创建启动master的actorsystem,与多个worker的actorsystem;
stop方法用于关闭清理master的actorsystem,与多个worker的actorsystem;

def start(): Array[String] = {
  logInfo("Starting a local Spark cluster with " + numWorkers + " workers.")
  // Disable REST server on Master in this mode unless otherwise specified
  val _conf = conf.clone()
    .setIfMissing("spark.master.rest.enabled", "false")
    .set(config.SHUFFLE_SERVICE_ENABLED.key, "false")
  /* Start the Master */
  val (rpcEnv, webUiPort, _) = Master.startRpcEnvAndEndpoint(localHostname, 0, 0, _conf)
  masterWebUIPort = webUiPort
  masterRpcEnvs += rpcEnv
  val masterUrl = "spark://" + Utils.localHostNameForURI() + ":" + rpcEnv.address.port
  val masters = Array(masterUrl)
  /* Start the Workers */
  for (workerNum <- 1 to numWorkers) {
    val workerEnv = Worker.startRpcEnvAndEndpoint(localHostname, 0, 0, coresPerWorker,
      memoryPerWorker, masters, null, Some(workerNum), _conf)
    workerRpcEnvs += workerEnv
  }
  masters
}
首先,disable rest 服务器;然后启动master;启动worker;返回master

启动master
def startRpcEnvAndEndpoint(
    host: String,
    port: Int,
    webUiPort: Int,
    conf: SparkConf): (RpcEnv, Int, Option[Int]) = {
  val securityMgr = new SecurityManager(conf)
  val rpcEnv = RpcEnv.create(SYSTEM_NAME, host, port, conf, securityMgr)
  val masterEndpoint = rpcEnv.setupEndpoint(ENDPOINT_NAME,
    new Master(rpcEnv, rpcEnv.address, webUiPort, securityMgr, conf))
  val portsResponse = masterEndpoint.askSync[BoundPortsResponse](BoundPortsRequest)
  (rpcEnv, portsResponse.webUIPort, portsResponse.restPort)
}

这一块,首先初始