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

LocalSparkCluster.scala中有实现方式

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
Master.scala中的 onStart()方法;

这一块,首先初始化 securityMgr,rpcEnv,masterEndpoint等

private def timeOutDeadWorkers() {
  // Copy the workers into an array so we don't modify the hashset while iterating through it
  val currentTime = System.currentTimeMillis()
  val toRemove = workers.filter(_.lastHeartbeat < currentTime - WORKER_TIMEOUT_MS).toArray
  for (worker <- toRemove) {
    if (worker.state != WorkerState.DEAD) {
      logWarning("Removing %s because we got no heartbeat in %d seconds".format(
        worker.id, WORKER_TIMEOUT_MS / 1000))
      removeWorker(worker)
    } else {
      if (worker.lastHeartbeat < currentTime - ((REAPER_ITERATIONS + 1) * WORKER_TIMEOUT_MS)) {
        workers -= worker // we've seen this DEAD worker in the UI, etc. for long enough; cull it
      }
    }
  }
}
初始化 超时的失效的工作节点
需要remove的标准:上一次心跳的时间间距超过汇报时间
如果workerinfo的状态不是dead,等待时间,移除;然后,根据心跳,来干掉worker
启动webUI,masterMetricSystem,applicationMetricsSystem,然后给masterMetricsSystem和applicationMetricsSystem
创建servletcontexthandler并且注册到webUI
选择持久化引擎
选择领导选举代理;

收到electedleader后,会进行选举操作


private def removeWorker(worker: WorkerInfo) {
  logInfo("Removing worker " + worker.id + " on " + worker.host + ":" + worker.port)
  worker.setState(WorkerState.DEAD)
  idToWorker -= worker.id
  addressToWorker -= worker.endpoint.address
  if (reverseProxy) {
    webUi.removeProxyTargets(worker.id)
  }
  for (exec <- worker.executors.values) {
    logInfo("Telling app of lost executor: " + exec.id)
    exec.application.driver.send(ExecutorUpdated(
      exec.id, ExecutorState.LOST, Some("worker lost"), None, workerLost = true))
    exec.state = ExecutorState.LOST
    exec.application.removeExecutor(exec)
  }
  for (driver <- worker.drivers.values) {
    if (driver.desc.supervise) {
      logInfo(s"Re-launching ${driver.id}")
      relaunchDriver(driver)
    } else {
      logInfo(s"Not re-launching ${driver.id} because it was not supervised")
      removeDriver(driver.id, DriverState.ERROR, None)
    }
  }
  persistenceEngine.removeWorker(worker)
}

private def removeDriver(
    driverId: String,
    finalState: DriverState,
    exception: Option[Exception]) {
  drivers.find(d => d.id == driverId) match {
    case Some(driver) =>
      logInfo(s"Removing driver: $driverId")
      drivers -= driver
      if (completedDrivers.size >= RETAINED_DRIVERS) {
        val toRemove = math.max(RETAINED_DRIVERS / 10, 1)
        completedDrivers.trimStart(toRemove)
      }
      completedDrivers += driver
      persistenceEngine.removeDriver(driver)
      driver.state = finalState
      driver.exception = exception
      driver.worker.foreach(w => w.removeDriver(driver))
      schedule()
    case None =>
      logWarning(s"Asked to remove unknown driver: $driverId")
  }
}

Master.scala中的实现
case ElectedLeader =>
      val (storedApps, storedDrivers, storedWorkers) = persistenceEngine.readPersistedData(rpcEnv)
      state = if (storedApps.isEmpty && storedDrivers.isEmpty && storedWorkers.isEmpty) {
        RecoveryState.ALIVE
      } else {
        RecoveryState.RECOVERING
      }
      logInfo("I have been elected leader! New state: " + state)
      if (state == RecoveryState.RECOVERING) {
        beginRecovery(storedApps, storedDrivers, storedWorkers)
        recoveryCompletionTask = forwardMessageThread.schedule(new Runnable {
          override def run(): Unit = Utils.tryLogNonFatalError {
            self.send(CompleteRecovery)
          }
        }, WORKER_TIMEOUT_MS, TimeUnit.MILLISECONDS)
      }

ElectedLeader,首先获取 storedApps, storedDrivers, storedWorkers
然后,获取状态
如果需要恢复,那么开始恢复
完成后有提示

beginRecovery的实现也在Master.scala中
private def beginRecovery(storedApps: Seq[ApplicationInfo], storedDrivers: Seq[DriverInfo],
    storedWorkers: Seq[WorkerInfo]) {
  for (app <- storedApps) {
    logInfo("Trying to recover app: " + app.id)
    try {
      registerApplication(app)
      app.state = ApplicationState.UNKNOWN
      app.driver.send(MasterChanged(self, masterWebUiUrl))
    } catch {
      case e: Exception => logInfo("App " + app.id + " had exception on reconnect")
    }
  }
  for (driver <- storedDrivers) {
    // Here we just read in the list of drivers. Any drivers associated with now-lost workers
    // will be re-launched when we detect that the worker is missing.
    drivers += driver
  }
  for (worker <- storedWorkers) {
    logInfo("Trying to recover worker: " + worker.id)
    try {
      registerWorker(worker)
      worker.state = WorkerState.UNKNOWN
      worker.endpoint.send(MasterChanged(self, masterWebUiUrl))
    } catch {
      case e: Exception => logInfo("Worker " + worker.id + " had exception on reconnect")
    }
  }
}

首先,针对storedApps的每一个app,尝试注册app,然后初始化app.state,app的driver发送信息
对于driver,增加driver
然后对于storedworkers的每一个worker,尝试注册worker,获取worker状态,利用endpoint发送信息

启动worker
创建,启动worker的actorsystem;每个worker的actorsystem都要注册自身的worker;
同时每个worker的actorsystem都要注册到workeractorsystems缓存

注册worker时,触发 onStart
订阅remotinglifecycleevent,坚挺远程客户端断开连接
创建工作目录;启动shuffleservice
创建workerwebui,然后启动
将worker注册到master
启动metricssystem

registerWithMaster(),是为了将worker注册到master中;调用tryRegisterAllMasters()方法
private def tryRegisterAllMasters(): Array[JFuture[_]] = {
  masterRpcAddresses.map { masterAddress =>
    registerMasterThreadPool.submit(new Runnable {
      override def run(): Unit = {
        try {
          logInfo("Connecting to master " + masterAddress + "...")
          val masterEndpoint = rpcEnv.setupEndpointRef(masterAddress, Master.ENDPOINT_NAME)
          sendRegisterMessageToMaster(masterEndpoint)
        } catch {
          case ie: InterruptedException => // Cancelled
          case NonFatal(e) => logWarning(s"Failed to connect to master $masterAddress", e)
        }
      }
    })
  }
}

master收到registerworker消息后,处理步骤:
创建workerinfo
注册workerinfo
向worker发送registeredworker消息,表示注册完成
调用schedule方法进行资源调度

注册workerinfo,其实就是将其添加到workersHashSet[WorkerInfo]中,并且更新worker id和worker以及workeraddress等

worker接受registeredworker消息的处理逻辑,步骤:
标记注册成功
调用changeMaster方法,更新activeMasterUrl等状态
启动定时调度,给自己发送sendheartbeat消息

master收到heartbeat消息后的实现也在Master中

local-cluster模式下,有一个Master和多个worker,位于同一个JVM,通过各自启动的actorsystem通信

6.2.2 CoarseGrainedSchedulerBackend启动
local-cluster模式,除了创建TaskScheduler的时候与local不同,启动taskScheduler时,也不同
local-cluster模式中,backend为SparkDeploySchedulerBackend.

CoarseGrainedSchedulerBackend的start方法的执行过程如下:
调用父类 CoarseGrainedSchedulerBackend 的start方法;
进行参数,Java选项,类路径的设置

启动AppClient;新版本中应该是 StandaloneAppClient
主要用来代表Application和Master通信
appclient启动时,会向driver的actorsystem注册clientactor

向ActorSystem注册时,先调用prestart方法;
override def onStart(): Unit = {
  try {
    registerWithMaster(1)
  } catch {
    case e: Exception =>
      logWarning("Failed to connect to master", e)
      markDisconnected()
      stop()
  }
}

registerWithMaster,有nthretry=1,说明重试1此
private def registerWithMaster(nthRetry: Int) {
  registerMasterFutures.set(tryRegisterAllMasters())
  registrationRetryTimer.set(registrationRetryThread.schedule(new Runnable {
    override def run(): Unit = {
      if (registered.get) {
        registerMasterFutures.get.foreach(_.cancel(true))
        registerMasterThreadPool.shutdownNow()
      } else if (nthRetry >= REGISTRATION_RETRIES) {
        markDead("All masters are unresponsive! Giving up.")
      } else {
        registerMasterFutures.get.foreach(_.cancel(true))
        registerWithMaster(nthRetry + 1)
      }
    }
  }, REGISTRATION_TIMEOUT_SECONDS, TimeUnit.SECONDS))
}

首先设置重试次数;设置线程

创建applicationinfo的实现如下
private def createApplication(desc: ApplicationDescription, driver: RpcEndpointRef):
    ApplicationInfo = {
  val now = System.currentTimeMillis()
  val date = new Date(now)
  val appId = newApplicationId(date)
  new ApplicationInfo(now, appId, desc, date, driver, defaultCores)
}

ApplicationInfo.scala中的实现,调用了init方法
private def init() {
  state = ApplicationState.WAITING
  executors = new mutable.HashMap[Int, ExecutorDesc]
  coresGranted = 0
  endTime = -1L
  appSource = new ApplicationSource(this)
  nextExecutorId = 0
  removedExecutors = new ArrayBuffer[ExecutorDesc]
  executorLimit = desc.initialExecutorLimit.getOrElse(Integer.MAX_VALUE)
}
声明了一系列参数

注册application时:
private def registerApplication(app: ApplicationInfo): Unit = {
  val appAddress = app.driver.address
  if (addressToApp.contains(appAddress)) {
    logInfo("Attempted to re-register application at same address: " + appAddress)
    return
  }
  applicationMetricsSystem.registerSource(app.appSource)
  apps += app
  idToApp(app.id) = app
  endpointToApp(app.driver) = app
  addressToApp(appAddress) = app
  waitingApps += app
}
更新各种关系,包括appid, appdriver, appaddress等


向standaloneclientactor发送注册消息后,
case RegisteredApplication(appId_, masterRef) =>
  // FIXME How to handle the following cases?
  // 1. A master receives multiple registrations and sends back multiple
  // RegisteredApplications due to an unstable network.
  // 2. Receive multiple RegisteredApplication from different masters because the master is
  // changing.
  appId.set(appId_)
  registered.set(true)
  master = Some(masterRef)
  listener.connected(appId.get)
更新appid;标识当前application注册到maser;
调用connected方法,更新appId,调用notifycontext方法标识application注册完成

6.2.4 资源调度
master,worker,application的启动和注册,executor是计算资源,但是好像没有体现.executor是什么时候创建的?application又是什么时候和executor取得联系的?
executor什么时候分给application处理任务的?

老版本的schedule方法,现在的版本是startExecutorsOnWorkers
private def startExecutorsOnWorkers(): Unit = {
  // Right now this is a very simple FIFO scheduler. We keep trying to fit in the first app
  // in the queue, then the second app, etc.
  for (app <- waitingApps if app.coresLeft > 0) {
    val coresPerExecutor: Option[Int] = app.desc.coresPerExecutor
    // Filter out workers that don't have enough resources to launch an executor
    val usableWorkers = workers.toArray.filter(_.state == WorkerState.ALIVE)
      .filter(worker => worker.memoryFree >= app.desc.memoryPerExecutorMB &&
        worker.coresFree >= coresPerExecutor.getOrElse(1))
      .sortBy(_.coresFree).reverse
    val assignedCores = scheduleExecutorsOnWorkers(app, usableWorkers, spreadOutApps)
    // Now that we've decided how many cores to allocate on each worker, let's allocate them
    for (pos <- 0 until usableWorkers.length if assignedCores(pos) > 0) {
      allocateWorkerResourceToExecutors(
        app, assignedCores(pos), coresPerExecutor, usableWorkers(pos))
    }
  }
}

资源调度两个步骤:逻辑分配,物理分配

计算资源,逻辑分配;对cpu进行分配;将当前application的cpu核数需求分配到所有worker,内存不满足的过滤掉
1,过滤处所有可用的worker
2,对于过滤得到的worker按照其空闲内核数倒序排列
3,实际需要分配的内核数=min(application需要的内核数,过滤后空闲内核数之和)
4,如果需要分配的内核>0,逐个从worker中分配直到最后worker;然后从头再次轮询分配;直到application需要内核=0

计算资源物理分配
给application物理分配worker的内存和核数

addExecutor,ApplicationInfo.scala中的实现

然后,调用master的launchExecutor方法来实现;

worker收到launchexecutor消息后的处理逻辑:Worker.scala中的 case LaunchExecutor(masterUrl, appId, execId, appDesc, cores_, memory_)
创建executor的工作目录
创建application的工作目录;当application完成时,此目录会被删除
创建并启动executorrunner
向master发送executorstatechanged消息

启动ExecutorRunner的时候实际创建了线程workerThread和shutdownHook;
ExecutorRunner.scala中实现的

workerThread执行过程中,主要调用了 fetchAndRunExecutor 方法;ExecutorRunner.scala中的实现如下

CoarseGrainedExecutorBackend.scala main方法
调用了run方法
1,初始化log
2,获取各种spark属性,包括executorConf,fetcher,driver,  config,props等
3,创建sparkenv
4,注册 CoarseGrainedExecutorBackend 到rpcEnv中
5,注册 WorkerWatcher 到 rpcEnv中

调用onStart方法;
override def onStart() {
  logInfo("Connecting to driver: " + driverUrl)
  rpcEnv.asyncSetupEndpointRefByURI(driverUrl).flatMap { ref =>
    // This is a very fast action so we can use "ThreadUtils.sameThread"
    driver = Some(ref)
    ref.ask[Boolean](RegisterExecutor(executorId, self, hostname, cores, extractLogUrls))
  }(ThreadUtils.sameThread).onComplete {
    // This is a very fast action so we can use "ThreadUtils.sameThread"
    case Success(msg) =>
      // Always receive `true`. Just ignore it
    case Failure(e) =>
      exitExecutor(1, s"Cannot register with driver: $driverUrl", e, notifyDriver = false)
  }(ThreadUtils.sameThread)
}
发送registeredexecutor消息
1,发送registeredexecutor消息,收到后,创建executor
2,worker接到launchexecutor消息后,创建executor目录,创建application本地目录,创建并启动executorrunner;最后向master发送executorstatechanged
3,executorrunner创建并运行线程workerthread
4,coarsegrainedexecutorbackend进程向driver发送 retrievesparkprops
5,driver收到retrievesparkprops 消息后,向 coarsegrainedexecutorbackend 进程发送spark属性;coarsegrainedexecutorbackend进程最后创建自身需要的actorsystem
6,coarsegrainedexecutorbackend 进程向actorsystem注册 coarsegrainedexecutorbackend,触发onstart,coarsegrainedexecutorbackend,onstart方法,向driveractor发送registerexecutor消息
7,driveractor接到registerexecutor消息后,先向 coarsegrainedexecutorbackend 发送registeredexecutor消息,更新executor信息等;注册到driver的executor的总数,创建executordata并且注册到map中
8,coarsegrainedexecutorbackend,收到registeredexecutor消息后创建executor
9,coarsegrainedexecutorbackend 进程向刚刚启动的actorsystem注册workerwatcher,注册workerwatcher时候触发onstart;然后向sendheartbeat消息初始化连接
10,worker收到 sendheartbeat消息后,向master发送heatbeat消息;master收到heartbeat消息后,如果发现worker没有注册过,则向worker发送 reconnectworker消息,要求worker重新想master注册

6.2.5 local-cluster模式的任务执行

所有的 actor-> Endpoint

driveractor->driverEndpoint

发送reviveoffers到 driverEndpoint;
driverEndpoint类,在 coarsegrainedschedulerbackend中;
收到消息后,调用 makeoffers
makeoffers的实现,也在 coarsegrainedschedulerbackend.scala 中
需要确保,在运行的时候,没有executors被杀掉
private def makeOffers() {
  // Make sure no executor is killed while some task is launching on it
  val taskDescs = CoarseGrainedSchedulerBackend.this.synchronized {
    // Filter out executors under killing
    val activeExecutors = executorDataMap.filterKeys(executorIsAlive)
    val workOffers = activeExecutors.map { case (id, executorData) =>
      new WorkerOffer(id, executorData.executorHost, executorData.freeCores)
    }.toIndexedSeq
    scheduler.resourceOffers(workOffers)
  }
  if (!taskDescs.isEmpty) {
    launchTasks(taskDescs)
  }
}
首先,过滤掉active的executors;
将executordata,转换为workeroffer;
利用resourceoffers,给当前任务分配executor
如果taskdescs非空,调用launchtasks

调用 launchtasks,返回一系列 resource offers
1,序列化 TaskDescription;
2,取出 ExecutorData信息;将executordata描述的空闲cpu-任务占用的核数
3,向 executor所在 CoarseGrainedExecutorBackend 发送launchtask信息

收到 LaunchTask后,走
case LaunchTask(data) =>
  if (executor == null) {
    exitExecutor(1, "Received LaunchTask command but executor was null")
  } else {
    val taskDesc = TaskDescription.decode(data.value)
    logInfo("Got assigned task " + taskDesc.taskId)
    executor.launchTask(this, taskDesc)
  }
反序列化,然后launchtask


总结local-cluster模式的任务执行过程:
partition数量为n,会启动n个 CoarseGrainedExecutorBackend进程, n个 ShuffleMapTask, 分别分配到n个进程中执行

local-cluster 和local模式的执行任务过程很类似;区别是 local-cluster模式的每个worker会启动多个 CoarseGrainedExecutorBackend进程; ExecutorBackend 和Executor 都再
CoarseGrainedExecutorBackend 的JVM进程中.

6.3 Standalone部署模式

local模式只有 Driver 和Executor,在同一个JVM进程中;local-cluster模式的Driver,Master,Worker,也在同一个JVM中.所以local模式和local-cluster模式便于开发,但是生产环境中不适合.

Standalone模式的特点:
Driver在集群外,可以是任意的客户端应用程序(用来控制?)
Master部署在单独的进程中,甚至应该在单独的机器节点.Master有多个,但是最多只有1个处于激活状态.
Worker部署在单独的进程中

6.3.1 启动Standalone模式
