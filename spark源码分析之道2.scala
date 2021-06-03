3,存储体系
	3.1 存储体系概述
		3.1.1 块管理器 BlockManager 的实现
			块管理器 BlockManager, 是Spark 存储体系的核心组件
			DriverApplication 和Executor 都会创建 BlockManager; BlockManager的实现如下
				private[spark] class BlockManager(
				    executorId: String,
				    rpcEnv: RpcEnv,
				    val master: BlockManagerMaster,
				    val serializerManager: SerializerManager,
				    val conf: SparkConf,
				    memoryManager: MemoryManager,
				    mapOutputTracker: MapOutputTracker,
				    shuffleManager: ShuffleManager,
				    val blockTransferService: BlockTransferService,
				    securityManager: SecurityManager,
				    numUsableCores: Int)
				  extends BlockDataManager with BlockEvictionHandler with Logging {

				  private[spark] val externalShuffleServiceEnabled =
				    conf.get(config.SHUFFLE_SERVICE_ENABLED)
				  private val remoteReadNioBufferConversion =
				    conf.getBoolean("spark.network.remoteReadNioBufferConversion", false)

				  val diskBlockManager = {
				    // Only perform cleanup if an external service is not serving our shuffle files.
				    val deleteFilesOnStop =
				      !externalShuffleServiceEnabled || executorId == SparkContext.DRIVER_IDENTIFIER
				    new DiskBlockManager(conf, deleteFilesOnStop)
				  }

				  // Visible for testing
				  private[storage] val blockInfoManager = new BlockInfoManager
    			  private val futureExecutionContext = ExecutionContext.fromExecutorService(
    			    ThreadUtils.newDaemonCachedThreadPool("block-manager-future", 128))
    			  // Actual storage of where blocks are kept
    			  private[spark] val memoryStore =
    			    new MemoryStore(conf, blockInfoManager, serializerManager, memoryManager, this)
    			  private[spark] val diskStore = new DiskStore(conf, diskBlockManager, securityManager)
    			  memoryManager.setMemoryStore(memoryStore)
			看起来,更换了一些操作 blockinfo->blockInfoManager
			tachyonStore,似乎被取消了
			接着是shuffleclient等
				var blockManagerId: BlockManagerId = _
				// Address of the server that serves this executor's shuffle files. This is either an external
				// service, or just our own Executor's BlockManager.
				private[spark] var shuffleServerId: BlockManagerId = _
				// Client to read other executors' shuffle files. This is either an external service, or just the
				// standard BlockTransferService to directly connect to other Executors.
				private[spark] val shuffleClient = if (externalShuffleServiceEnabled) {
				  val transConf = SparkTransportConf.fromSparkConf(conf, "shuffle", numUsableCores)
				  new ExternalShuffleClient(transConf, securityManager,
				    securityManager.isAuthenticationEnabled(), conf.get(config.SHUFFLE_REGISTRATION_TIMEOUT))
				} else {
				  blockTransferService
				}
			然后是 slaveActor->slaveEndpoint, cleaner 似乎被取消了

			BlockManager 组成成分
				ShuffleClient
				BlockManagerMaster
				DiskBlockManager
				MemoryStore
				DiskStore
				tachyonStore
				非广播block清理器 metadataCleaner, 广播block清理器 broadcastCleaner
				压缩算法实现 CompressionCodec

			BlockManager 要生效,首先需要初始化
				def initialize(appId: String): Unit = {
				  blockTransferService.init(this)
				  shuffleClient.init(appId)
				  blockReplicationPolicy = {
				    val priorityClass = conf.get(
				      "spark.storage.replication.policy", classOf[RandomBlockReplicationPolicy].getName)
				    val clazz = Utils.classForName(priorityClass)
				    val ret = clazz.newInstance.asInstanceOf[BlockReplicationPolicy]
				    logInfo(s"Using $priorityClass for block replication policy")
				    ret
				  }
				  val id =
				    BlockManagerId(executorId, blockTransferService.hostName, blockTransferService.port, None)
				  val idFromMaster = master.registerBlockManager(
				    id,
				    maxOnHeapMemory,
				    maxOffHeapMemory,
				    slaveEndpoint)
				  blockManagerId = if (idFromMaster != null) idFromMaster else id
				  shuffleServerId = if (externalShuffleServiceEnabled) {
				    logInfo(s"external shuffle service port = $externalShuffleServicePort")
				    BlockManagerId(executorId, blockTransferService.hostName, externalShuffleServicePort)
				  } else {
				    blockManagerId
				  }
				  // Register Executors' configuration with the local shuffle service, if one should exist.
				  if (externalShuffleServiceEnabled && !blockManagerId.isDriver) {
				    registerWithExternalShuffleServer()
				  }
				  logInfo(s"Initialized BlockManager: $blockManagerId")
				}
			blockTransferService初始化;shuffleClient初始化
			id 和 idFromMaster 的确定
			blockManagerId 和 shuffleServerId 初始化; 当有外部的 ShuffleService时, 创建新的 BlockManagerId 否则ShuffleServerId默认使用当前BlockManager的BlockManagerID
			向 BlockManagerMaster 注册 BlockManagerId
		3.1.2 Spark存储体系
			BlockManager->BlockManagerID

			DiskStore 			MemStore
			DiskBlockManager 	UnifiedMemoryManager
