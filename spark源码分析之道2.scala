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

			Executor 的 BlockManager Driver 的 BlockManager 进行通信,例如注册 BlockManager,更新Block信息,获取Block在的BlockManager,删除Executor等
			BlockManager的读写操作,从各类存储介质中取数,寸数
			MemoryStore不足,会写入DiskStore.DiskStore;而DiskStore实际依赖于DiskBlockManager
			访问远端节点的Executor的BlockManager

			目前Spark支持 HDFS,AmazonS3两种主流分布式存储系统
			https://www.sohu.com/a/441840842_355140
			Spark定义了抽象类BlockStore, 目前BlockStore具体实现包括MemoryStore,DiskStore,TachyonStore


		3.2 shuffle服务&客户端
			Netty实现的网络服务组件,于存储体系的重要性:Spark是分布式部署的,每个Task最终都运行在不同机器节点上.
			map任务的输出结果存在map任务所在机器的存储体系中,但是reduce任务极有可能不在同一机器上运行,所以需要netty,网络服务,实现远程下载map任务的中间输出

			shuffleClient, 不仅是客户端,不光将shuffle文件上传到executor或者下载到本地客户端,还提供了可以被其他executor访问的shuffle服务
			和Yarn一样,都是用netty作为shuffleserver

			BlockTransferService只有在init方法调用,初始化后,才提供服务

			NettyBlockTransferService的初始化步骤:
				SparkEnv的 val blockTransferService = new NettyBlockTransferService(conf, securityManager, bindAddress, advertiseAddress, blockManagerPort, numUsableCores) ->
				NettyBlockTransferService.init
				override def init(blockDataManager: BlockDataManager): Unit = {
				    val rpcHandler = new NettyBlockRpcServer(conf.getAppId, serializer, blockDataManager)
				    var serverBootstrap: Option[TransportServerBootstrap] = None
				    var clientBootstrap: Option[TransportClientBootstrap] = None
				    if (authEnabled) {
				      serverBootstrap = Some(new AuthServerBootstrap(transportConf, securityManager))
				      clientBootstrap = Some(new AuthClientBootstrap(transportConf, conf.getAppId, securityManager))
				    }
				    transportContext = new TransportContext(transportConf, rpcHandler)
				    clientFactory = transportContext.createClientFactory(clientBootstrap.toSeq.asJava)
				    server = createServer(serverBootstrap.toList)
				    appId = conf.getAppId
				    logInfo(s"Server created on ${hostName}:${server.getPort}")
				  }

			Block的RPC符文,构造TransportContext(类似SparkContext),创建RPC客户端工厂 TransportClientFactory, Netty服务器TransportServer
			现在的Netty好像被rpc替代了
		
			3.2.1 block 的 RPC 服务
				当map任务与reduce任务处于不同节点,reduce任务要从远端节点下载map任务的输出
				因此,NettyBlockRpcServer 提供了下载Block的功能; 同时为了容错,需要将Block的数据备份到其他节点上
				所以 NettyBlockRpcServer 还提供了上传文件的RPC服务
				RPC,Remote Procedure Call,远程过程调用.允许一台计算机调用另一台计算机上的程序得到结果,在代码中不需要做额外的编程,就像在本地调用一样.

				NettyBlockRpcServer类的实现 NettyBlockRpcServer.scala
				全部代码不在此展示了

			3.2.2 构造传输上下文 TransportContext
				用于维护传输上下文
				TransportContext.scala脚本中有实现代码
					public TransportContext(
					    TransportConf conf,
					    RpcHandler rpcHandler,
					    boolean closeIdleConnections) {
					  this.conf = conf;
					  this.rpcHandler = rpcHandler;
					  this.closeIdleConnections = closeIdleConnections;
					}
				TransportContext 既可以创建Netty服务也可创建Netty访问客户端;
					TransportConf,参数,控制Netty框架提供的shuffle的IO交互的客户端和服务端线程数量
					RpcHandler, 负责shuffle的IO服务的接到Rpc请求后,提供打开block或者上次block的Rpc处理; 即为 NettyBlockRpcServer
					decoder,encoder,解析和加密的功能,似乎新版本中取消了

			3.2.3 Rpc客户端工程TransportClientFactory
				TransportClientFactory 是创建Netty客户端 TransportClient的工厂类;TransportClient,用于向Netty服务端发送Rpc请求
					public TransportClientFactory createClientFactory(List<TransportClientBootstrap> bootstraps) {
    					return new TransportClientFactory(this, bootstraps);
  					}
  				实现方法存在多种;
  				TransportClientBootstrap, 缓存客户端列表
  				connection pool,连接池,缓存客户端连接
  				numConnectionsPerPeer, 节点之间,取数据的连接数;
  				socketChannelClass,客户端channel被创建时使用的类;
  				workerGroup,worker组;
  				pooledAllocator,取代了之前的poolAllocator;汇集ByteBuf但是对本地线程缓存禁用的分配器
					public TransportClientFactory(
					    TransportContext context,
					    List<TransportClientBootstrap> clientBootstraps) {
					  this.context = Preconditions.checkNotNull(context);
					  this.conf = context.getConf();
					  this.clientBootstraps = Lists.newArrayList(Preconditions.checkNotNull(clientBootstraps));
					  this.connectionPool = new ConcurrentHashMap<>();
					  this.numConnectionsPerPeer = conf.numConnectionsPerPeer();
					  this.rand = new Random();
					  IOMode ioMode = IOMode.valueOf(conf.ioMode());
					  this.socketChannelClass = NettyUtils.getClientChannelClass(ioMode);
					  this.workerGroup = NettyUtils.createEventLoop(
					      ioMode,
					      conf.clientThreads(),
					      conf.getModuleName() + "-client");
					  this.pooledAllocator = NettyUtils.createPooledByteBufAllocator(
					    conf.preferDirectBufs(), false /* allowCache */, conf.clientThreads());
					  this.metrics = new NettyMemoryMetrics(
					    this.pooledAllocator, conf.getModuleName() + "-client", conf);
					}
			3.2.4 Netty 服务器 TransportServer
				TransportServer 提供 Netty 实现的服务器端;用于提供RPC服务; 代码如下
					public TransportServer createServer(int port, List<TransportServerBootstrap> bootstraps) {
					  return new TransportServer(this, null, port, rpcHandler, bootstraps);
					}
				TransportServer类的构造如下
					public TransportServer(
					    TransportContext context,
					    String hostToBind,
					    int portToBind,
					    RpcHandler appRpcHandler,
					    List<TransportServerBootstrap> bootstraps) {
					  this.context = context;
					  this.conf = context.getConf();
					  this.appRpcHandler = appRpcHandler;
					  this.bootstraps = Lists.newArrayList(Preconditions.checkNotNull(bootstraps));
					  boolean shouldClose = true;
					  try {
					    init(hostToBind, portToBind);
					    shouldClose = false;
					  } finally {
					    if (shouldClose) {
					      JavaUtils.closeQuietly(this);
					    }
					  }
					}
				init方法,用于TransportServer的初始化;通过调用NEtty框架的EventLoopEvent,ServerBootstrap等API创建shuffle的IO交互
					private void init(String hostToBind, int portToBind) {
					   IOMode ioMode = IOMode.valueOf(conf.ioMode());
					   EventLoopGroup bossGroup = NettyUtils.createEventLoop(ioMode, 1,
					     conf.getModuleName() + "-boss");
					   EventLoopGroup workerGroup =  NettyUtils.createEventLoop(ioMode, conf.serverThreads(),
					     conf.getModuleName() + "-server");
					   PooledByteBufAllocator allocator = NettyUtils.createPooledByteBufAllocator(
					     conf.preferDirectBufs(), true /* allowCache */, conf.serverThreads());
					   bootstrap = new ServerBootstrap()
					     .group(bossGroup, workerGroup)
					     .channel(NettyUtils.getServerChannelClass(ioMode))
					     .option(ChannelOption.ALLOCATOR, allocator)
					     .option(ChannelOption.SO_REUSEADDR, !SystemUtils.IS_OS_WINDOWS)
					     .childOption(ChannelOption.ALLOCATOR, allocator);
					    ...

				ServerBootstrap chldhandler方法,调用了TransportContext的initializePipeline
				initializePipeline 创建了TransportChannelHandler
					public TransportChannelHandler initializePipeline(
					    SocketChannel channel,
					    RpcHandler channelRpcHandler) {
					  try {
					    TransportChannelHandler channelHandler = createChannelHandler(channel, channelRpcHandler);
					    channel.pipeline()
					      .addLast("encoder", ENCODER)
					      .addLast(TransportFrameDecoder.HANDLER_NAME, NettyUtils.createFrameDecoder())
					      .addLast("decoder", DECODER)
					      .addLast("idleStateHandler", new IdleStateHandler(0, 0, conf.connectionTimeoutMs() / 1000))
					      // NOTE: Chunks are currently guaranteed to be returned in the order of request, but this
					      // would require more logic to guarantee if this were not part of the same event loop.
					      .addLast("handler", channelHandler);
					    ...

			3.2.5 获取线程shuffle文件
				NettyBlockTransferService 的fetchBlocks 方法 用于获取远程 shuffle文件 实际上利用了 NettyBlockTransferService中创建的Netty服务
					override def fetchBlocks(
					    host: String,
					    port: Int,
					    execId: String,
					    blockIds: Array[String],
					    listener: BlockFetchingListener,
					    tempFileManager: DownloadFileManager): Unit = {
					  logTrace(s"Fetch blocks from $host:$port (executor id $execId)")
					  try {
					    val blockFetchStarter = new RetryingBlockFetcher.BlockFetchStarter {
					      override def createAndStart(blockIds: Array[String], listener: BlockFetchingListener) {
					        val client = clientFactory.createClient(host, port)
					        new OneForOneBlockFetcher(client, appId, execId, blockIds, listener,
					          transportConf, tempFileManager).start()
					      }
					    }
					    val maxRetries = transportConf.maxIORetries()
					    if (maxRetries > 0) {
					      // Note this Fetcher will correctly handle maxRetries == 0; we avoid it just in case there's
					      // a bug in this code. We should remove the if statement once we're sure of the stability.
					      new RetryingBlockFetcher(transportConf, blockFetchStarter, blockIds, listener).start()
					    } else {
					      blockFetchStarter.createAndStart(blockIds, listener)
					    }
					  } catch {
					    case e: Exception =>
					      logError("Exception while beginning fetchBlocks", e)
					      blockIds.foreach(listener.onBlockFetchFailure(_, e))
					  }
					}
				实现方法,NettyBlockTransferService.fetchBlocks

			3.2.6 上传 shuffle文件
				NettyBlockTransferService 的uploadblock 实际上用于上传shuffle文件到远程executor
				利用了 NettyBlockTransferService
					override def uploadBlock(
					    hostname: String,
					    port: Int,
					    execId: String,
					    blockId: BlockId,
					    blockData: ManagedBuffer,
					    level: StorageLevel,
					    classTag: ClassTag[_]): Future[Unit] = {
					  val result = Promise[Unit]()
					  val client = clientFactory.createClient(hostname, port)
					  // StorageLevel and ClassTag are serialized as bytes using our JavaSerializer.
					  // Everything else is encoded using our binary protocol.
					  val metadata = JavaUtils.bufferToArray(serializer.newInstance().serialize((level, classTag)))
					  val asStream = blockData.size() > conf.get(config.MAX_REMOTE_BLOCK_SIZE_FETCH_TO_MEM)
					  val callback = new RpcResponseCallback {
					    override def onSuccess(response: ByteBuffer): Unit = {
					      logTrace(s"Successfully uploaded block $blockId${if (asStream) " as stream" else ""}")
					      result.success((): Unit)
					    }
					    override def onFailure(e: Throwable): Unit = {
					      logError(s"Error while uploading $blockId${if (asStream) " as stream" else ""}", e)
					      result.failure(e)
					    }
					  }
					  if (asStream) {
					    val streamHeader = new UploadBlockStream(blockId.name, metadata).toByteBuffer
					    client.uploadStream(new NioManagedBuffer(streamHeader), blockData, callback)
					  } else {
					    // Convert or copy nio buffer into array in order to serialize it.
					    val array = JavaUtils.bufferToArray(blockData.nioByteBuffer())
					    client.sendRpc(new UploadBlock(appId, execId, blockId.name, metadata, array).toByteBuffer,
					      callback)
					  }
					  result.future
					}
				流程是:
					创建客户端;将数据序列化;将ByteBuffer转化为数组,方便序列化
					可以作为stream,将数据上传;也可以转化成nio buffer,上传
					都是通过client上传的;stream时,uploadStream;bufferarray时,sendRpc

		3.3 BlockManagerMaster 对于 BlockManager的管理
			Driver上的 BlockManagerMaster, 对于存在Executor上面的BlockManager,统一管理
			但是如果 Driver和Executor 位于不同机器时,如何是好?
			Driver的BlockManagerMaster 会持有 BlockManagerMasterActor, 所有executor也会从ActorSystem中获取 BlockManagerMasterActor的引用
			所有 Executor 和Driver 关于BlockManager的交互都依赖它
			这里的 BlockManagerMasterActor 实际上应该是 Rpc了;Actor已经被取消了

			3.3.1 BlockManagerMasterActor
				BlockManagerMasterActor 只存在于Driver上;Executor从ActorSystem获取BlockManagerMasterActor的引用;
				然后给BlockManagerMasterActor发送消息,实现和Driver的交互
				新版本中,BlockManagerMasterActor被 BlockManagerMasterEndpoint替代
				BlockManagerMasterEndpoint,维护了很多缓存数据结构
				blockManagerInfo, blockmanagerid和blockmanager的信息
				blockmanageridbyexecutor, 缓存executorid与其拥有的blockmanagerid之间的映射关系
				blocklocation,缓存block与blockmanagerid的映射关系
				

			3.3.2 询问Driver并获取回复方法
				在Executor的BlockManagerMaster中,所有与Driver上BlockManagerMaster的交互方法最终都调用了askDriverwithReply

			3.3.3 向Blockmanagermaster注册blockmanagerid
				executor或者driver,本身的blockmanager在初始化时,需要向driver的blockmanager注册blockmanager信息

		3.4 磁盘块管理器
			3.4.1 DiskBlockManager的构造过程
				调用createLocalDirs;DiskBlockManager.scala;创建二维数组subdir,用来缓存一级目录localdirs和二级目录
				二级目录,数量根据配置spark.diskstore.subdirectories获取,默认64
					private[spark] val subDirsPerLocalDir = conf.getInt("spark.diskStore.subDirectories", 64)
					private val subDirs = Array.fill(localDirs.length)(new Array[File](subDirsPerLocalDir))
					private[spark] val localDirs: Array[File] = createLocalDirs(conf)
					  if (localDirs.isEmpty) {
					    logError("Failed to create any local dir.")
					    System.exit(ExecutorExitCode.DISK_STORE_FAILED_TO_CREATE_DIR)
					  }
					private def createLocalDirs(conf: SparkConf): Array[File] = {
					  Utils.getConfiguredLocalDirs(conf).flatMap { rootDir =>
					    try {
					      val localDir = Utils.createDirectory(rootDir, "blockmgr")
					      logInfo(s"Created local directory at $localDir")
					      Some(localDir)
					    } catch {
					      case e: IOException =>
					        logError(s"Failed to create local dir in $rootDir. Ignoring this directory.", e)
					        None
					    }
					  }
					}
				添加运行时环境结束时的钩子,用于在进程关闭时创建线程;通过调用DiskBlockManager的stop方法,清除一些临时目录
					private def addShutdownHook(): AnyRef = {
					  logDebug("Adding shutdown hook") // force eager creation of logger
					  ShutdownHookManager.addShutdownHook(ShutdownHookManager.TEMP_DIR_SHUTDOWN_PRIORITY + 1) { () =>
					    logInfo("Shutdown hook called")
					    DiskBlockManager.this.doStop()
					  }
					}
					/** Cleanup local dirs and stop shuffle sender. */
					private[spark] def stop() {
					  // Remove the shutdown hook.  It causes memory leaks if we leave it around.
					  try {
					    ShutdownHookManager.removeShutdownHook(shutdownHook)
					  } catch {
					    case e: Exception =>
					      logError(s"Exception while removing shutdown hook.", e)
					  }
					  doStop()
					}
				DiskBlockManager为什么要创建二级目录结构?这是因为二级目录,用于对文件进行散列存储;散列存储可以使得所有文件都随机存放,写入删除更快

			3.4.2 获取磁盘文件方法 getFile
				getFile方法,获取磁盘文件
					/** Looks up a file by hashing it into one of our local subdirectories. */
					// This method should be kept in sync with
					// org.apache.spark.network.shuffle.ExternalShuffleBlockResolver#getFile().
					def getFile(filename: String): File = {
					  // Figure out which local directory it hashes to, and which subdirectory in that
					  val hash = Utils.nonNegativeHash(filename)
					  val dirId = hash % localDirs.length
					  val subDirId = (hash / localDirs.length) % subDirsPerLocalDir
					  // Create the subdirectory if it doesn't already exist
					  val subDir = subDirs(dirId).synchronized {
					    val old = subDirs(dirId)(subDirId)
					    if (old != null) {
					      old
					    } else {
					      val newDir = new File(localDirs(dirId), "%02x".format(subDirId))
					      if (!newDir.exists() && !newDir.mkdir()) {
					        throw new IOException(s"Failed to create local dir in $newDir.")
					      }
					      subDirs(dirId)(subDirId) = newDir
					      newDir
					    }
					  }
				计算hash值;->根据hash值,与本地文件一级目录总数,求余数;记dirId;->hash值与本地文件一级目录总数求商数,此商数与二级目录数目求余数
				如果 dirId/subDirId目录存在,则获取dirId/subDirId目录下的文件,否则新建 dirId/subDirId 目录
			3.4.3 创建临时 Block 方法,createTempShuffleBlock
				当shuffleMapTask运行结束时,需要把中间结果临时保存;此时,调用 createTempShuffleBlock,作为临时Block,返回TempShuffleBlockId与其文件的对偶
					def createTempShuffleBlock(): (TempShuffleBlockId, File) = {
					  var blockId = new TempShuffleBlockId(UUID.randomUUID())
					  while (getFile(blockId).exists()) {
					    blockId = new TempShuffleBlockId(UUID.randomUUID())
					  }
					  (blockId, getFile(blockId))
					}

		3.5 磁盘存储DiskStore
			当MemoryStore没有足够空间,会使用DiskStore,存入磁盘;DiskStore集成BlockStore,实现了getBytes,putBytes,putArray,putIterator等
			diskStore.scala
			3.5.1 NIO读取方法 GetBytes
				通过DiskBlockManager的getFile方法获取文件 然后使用NIO将文件读取到ByteBuffer中

			3.5.2 NIO写入方法putBytes
				putBytes 方法的作用,是通过DiskBlockManager的getFile方法,获取文件;然后使用NIO的channel将ByteBuffer写入文件

			3.5.3 数组写入方法 putArray
			3.5.4 Iterator写入方法 putIterator
		3.6 内存存储 MemoryStore
			内存存储负责将没有序列化的Java对象数组或者序列化的ByteBuffer存储到内存中
				private[spark] class MemoryStore(
				    conf: SparkConf,
				    blockInfoManager: BlockInfoManager,
				    serializerManager: SerializerManager,
				    memoryManager: MemoryManager,
				    blockEvictionHandler: BlockEvictionHandler)
				  extends Logging {

				  // Note: all changes to memory allocations, notably putting blocks, evicting blocks, and
				  // acquiring or releasing unroll memory, must be synchronized on `memoryManager`!

				  private val entries = new LinkedHashMap[BlockId, MemoryEntry[_]](32, 0.75f, true)

				  // A mapping from taskAttemptId to amount of memory used for unrolling a block (in bytes)
				  // All accesses of this map are assumed to have manually synchronized on `memoryManager`
				  private val onHeapUnrollMemoryMap = mutable.HashMap[Long, Long]()
				  // Note: off-heap unroll memory is only used in putIteratorAsBytes() because off-heap caching
				  // always stores serialized values.
				  private val offHeapUnrollMemoryMap = mutable.HashMap[Long, Long]()

				  // Initial memory to request before unrolling any block
				  private val unrollMemoryThreshold: Long =
				    conf.getLong("spark.storage.unrollMemoryThreshold", 1024 * 1024)

				  /** Total amount of memory available for storage, in bytes. */
				  private def maxMemory: Long = {
				    memoryManager.maxOnHeapStorageMemory + memoryManager.maxOffHeapStorageMemory
				  }

				  if (maxMemory < unrollMemoryThreshold) {
				    logWarning(s"Max memory ${Utils.bytesToString(maxMemory)} is less than the initial memory " +
				      s"threshold ${Utils.bytesToString(unrollMemoryThreshold)} needed to store a block in " +
				      s"memory. Please configure Spark with more memory.")
				  }

				  logInfo("MemoryStore started with capacity %s".format(Utils.bytesToString(maxMemory)))

				  /** Total storage memory used including unroll memory, in bytes. */
				  private def memoryUsed: Long = memoryManager.storageMemoryUsed

				  /**
				   * Amount of storage memory, in bytes, used for caching blocks.
				   * This does not include memory used for unrolling.
				   */
				  private def blocksMemoryUsed: Long = memoryManager.synchronized {
				    memoryUsed - currentUnrollMemory
				  }
			MaxMemory = currentMemory + freeMemory
					  = currentMemory + maxUnrollMemory + otherMemory
					  = currentMemory + currentUnrollMemory + freeUnrollMemory + otherMemory
			3.6.1 putBytes
				老版本里面,要求序列化判断:如果Block可以被反序列化,即序列化Block然后putIterator;否则trytoput
				新版本中,脚本如下
					def putBytes[T: ClassTag](
					      blockId: BlockId,
					      size: Long,
					      memoryMode: MemoryMode,
					      _bytes: () => ChunkedByteBuffer): Boolean = {
					    require(!contains(blockId), s"Block $blockId is already present in the MemoryStore")
					    if (memoryManager.acquireStorageMemory(blockId, size, memoryMode)) {
					      // We acquired enough memory for the block, so go ahead and put it
					      val bytes = _bytes()
					      assert(bytes.size == size)
					      val entry = new SerializedMemoryEntry[T](bytes, memoryMode, implicitly[ClassTag[T]])
					      entries.synchronized {
					        entries.put(blockId, entry)
					      }
					      logInfo("Block %s stored as bytes in memory (estimated size %s, free %s)".format(
					        blockId, Utils.bytesToString(size), Utils.bytesToString(maxMemory - blocksMemoryUsed)))
					      true
					    } else {
					      false
					    }
				首先判断,是否获取了storage的Memory;如果获得了:
					获取字节数;
					序列化;
					put
			3.6.2 Iterator 写入方法,putIterator详解
				新版本不同于老版本了
				private[storage] def putIteratorAsValues[T](
				      blockId: BlockId,
				      values: Iterator[T],
				      classTag: ClassTag[T]): Either[PartiallyUnrolledIterator[T], Long] = {
					//首先初始化参数
					//申请内存,以开始展开内存
					keepUnrolling = reserveUnrollMemoryForThisTask(blockId, initialMemoryThreshold, MemoryMode.ON_HEAP)
					
					//如果一直在展开:
						warning
					//如果不是一直在展开:
						this block 占据的展开的内存 += initialMemoryThreshold

					//展开block,定期检查是否超过了threshold
					while (values.hasNext && keepUnrolling) {
					      vector += values.next()
					      if (elementsUnrolled % memoryCheckPeriod == 0) {
					        // If our vector's size has exceeded the threshold, request more memory
					        val currentSize = vector.estimateSize()
					        if (currentSize >= memoryThreshold) {
					          val amountToRequest = (currentSize * memoryGrowthFactor - memoryThreshold).toLong
					          keepUnrolling =
					            reserveUnrollMemoryForThisTask(blockId, amountToRequest, MemoryMode.ON_HEAP)
					          if (keepUnrolling) {
					            unrollMemoryUsedByThisBlock += amountToRequest
					          }
					          // New threshold is currentSize * memoryGrowthFactor
					          memoryThreshold += amountToRequest
					        }
					      }
					      elementsUnrolled += 1
					    }
			3.6.3 安全展开方法 unrollSafely;这一部分在putIterator中融合起来了
				为了防止写入内存数据过大,导致内存溢出:在正式写入内存之前,先用逻辑方式申请内存;申请成功,才写入;称为安全展开.
					elementsUnrolled = 展开的元素数量
					initialMemoryThreshold = 每个线程用来展开Block的初始内存阈值
					memoryCheckPeriod = 多次展开内存后,判断是否需要申请更多内存
					memoryThreshold = 当前线程保留的用于特殊展开操作的内存阈值
					memoryGrowthFactor = 内存请求因子
					previousMemoryThreshold = 之前当前线程已经展开的驻留的内存大小;当前线程增加的展开内存,最后会被释放
					vector = 跟踪展开内存信息
					maxUnrollMemory = 当前Driver或者Executor最多展开的Block占用的内存
					maxMemory = 当前Driver或者Executor的最大内存
					currentMemory = 当前Driver或者Executor已经使用的内存
					freeMemory = 当前Driver或者Executor未使用的内存
					currentUnrollMemory = unrollMemoryMap中,所有展开的block的内存和;当前Driver或者Executor所有线程展开的block的内存和
					unrollMemoryMap = 当前Driver或者Executor中所有线程展开的Block都存入此Map中;key=线程id,value=内存大小和
					currentSize = vector中跟踪的对象的总大小
					keepUnrolling = 标记是否有足够的内存来展开Block = freeMemory > currentUnrollMemory + memory(要分配的内存)

				展开内存的步骤
					申请 memoryThreshold的初始大小为 initialMemoryThreshold
					如果 Iterator[Any]中有元素,而且 keepUnrolling, 向vector中添加 Iterator[Any], elementsUnrolled += 1;反之跳步骤4
					如果 elementsUnrolled % memoryCheckPeriod == 0
						检查currentSize 是否 大于 memoryThreshold?大于
							申请内存;
							如果申请失败,但是 maxUnrollMemory > currentUnrollMemory->释放内存
						小于:
							不管
					根据是否将block完整放入内存,以数组或者迭代器形式返回vector数据
					最后,计算本次展开块实际占用的空间,amountToRelease,更新unrollMemoryMap中当前线程的内存大小

				unrollSafely 多次使用 reserveUnrollMemoryForThisTask, 以便给当前线程申请逻辑内存
					def reserveUnrollMemoryForThisTask(
					      blockId: BlockId,
					      memory: Long,
					      memoryMode: MemoryMode): Boolean = {
					    memoryManager.synchronized {
					      val success = memoryManager.acquireUnrollMemory(blockId, memory, memoryMode)
					      if (success) {
					        val taskAttemptId = currentTaskAttemptId()
					        val unrollMemoryMap = memoryMode match {
					          case MemoryMode.ON_HEAP => onHeapUnrollMemoryMap
					          case MemoryMode.OFF_HEAP => offHeapUnrollMemoryMap
					        }
					        unrollMemoryMap(taskAttemptId) = unrollMemoryMap.getOrElse(taskAttemptId, 0L) + memory
					      }
					      success
					    }
					  }
				首先获取unroll的内存大小;
				如果unroll的内存非空:
					获取id
					根据memorymode,判断
				然后,申请逻辑内存

			3.6.4 确认空闲内存方法 ensureFreeSpace
				ensureFreeSpace 确认是否有足够内存,if 不足,会释放被 MemoryEntry占用的内存
				变量:
					actualFreeMemory = 实际空闲的内存 = actualFreeMemory - currentUnrollMemory
					selectedBlocks = 已经选择要从内存中腾出的Block对应的BlockId的数组
					space = 需要腾出的内存大小
					entries = 用于存放Block
					blockIdToAdd = 将要添加的Block对应的blockId
				实现:
					space !> maxMemory
					如果 actualFreeMemory >= space, 说明内存足够;不用释放内存空间直接返回
					如果 actualFreeMemory + selectedMemory < space, 迭代 entries, 获得blockId
					actualFreeMemory + selectedMemory 大于等于 space, 说明可以腾出空间
					
			3.6.5 内存写入方法 putArray
				内存写入方法 putArray 首先对对象大小进行估算，然后写入内存
				如果 unrollSafely 返回的数据 匹配Left,arrayValues, 说明整个block可以一次性放入内存->调用putArray
				sizeestimator.estimate 用来估算对象大小,遍历对象和属性

			3.6.6 尝试写入内存方法 trytoPut
				block不支持序列化时,调用 tryToPut 方法

			3.6.7 获取内存数据方法 GetBytes
				从entries中获取memoryentry;如果 memoryentry 支持反序列化=>value反序列化后返回,否则对 memoryentry 的value复制后返回
					def getBytes(blockId: BlockId): Option[ChunkedByteBuffer] = {
					  val entry = entries.synchronized { entries.get(blockId) }
					  entry match {
					    case null => None
					    case e: DeserializedMemoryEntry[_] =>
					      throw new IllegalArgumentException("should only call getBytes on serialized blocks")
					    case SerializedMemoryEntry(bytes, _, _) => Some(bytes)
					  }
					}
			3.6.8 获取数据方法 getValues
			从内存中获取数据 从entries中获取memoryentry;并将blockid和value返回
				def getValues(blockId: BlockId): Option[Iterator[_]] = {
				  val entry = entries.synchronized { entries.get(blockId) }
				  entry match {
				    case null => None
				    case e: SerializedMemoryEntry[_] =>
				      throw new IllegalArgumentException("should only call getValues on deserialized blocks")
				    case DeserializedMemoryEntry(values, _, _) =>
				      val x = Some(values)
				      x.map(_.iterator)
				  }
				}

		3.7 Tachyon 存储 TachyonStore
			为啥要用 Tachyon?
				spark的基于HDFS的文件系统是基于磁盘的,读写效率低下;
				spark的计算引擎和存储体系,都位于executor的同一进程中,if计算挂了,存储体系缓存的数据也会挂
				不同的spark,任务可能访问同样的数据;重复加载,导致数据对象过多,GC时间过长
			3.7,1 Tachyon
				Tachyon 是位于现有大数据计算框架和大数据存储系统之间的独立一层

				Tachyon 采用 master-worker架构;支持zk进行容错,用zk管理文件的元数据信息;监控各个taychon worker的状态
				每个tachyon worker启动一个守护进程,管理本地ramdisk
				ramdisk实际上是tachyon集群的内存部分
				tachyon采用和spark类似的RDD的操作,利用lineage,和异步记录的checkpoint来恢复数据
			3.7.2 TachyonStore的使用
				Spark源码自带的SparkTachyonHdfsLR
				不过现在2.4.7版本中已经几乎找不到Tachyon的痕迹了.pass

		3.8 块管理器 BlockManager
			3.8.1 移出内存方法 dropFromMemory
				内存不足的时候,需要腾出部分内存空间 dropFromMemory 实现了这个功能; spark.storage.BlockManager.scala
				1,从blockinfo中检查,是否存在需要迁移的blockid->如果存在,从blockinfo中获取block的storagelevel
				2,if StorageLevel 允许存入内存,且 diskstore 中不存在此文件;调用 putarray或者putbytes方法,存到硬盘中
				3,从memorystore清除blockid对应的block
				4,使用 getcurrentblockstatus 方法获取block的最新状态 if此block的tellmaster未true,调用reportblockstatus方法,给blockmanagermasteractor报告状态

			3.8.2 状态报告方法 reportblockstatus
				private def reportBlockStatus(
				      blockId: BlockId,
				      status: BlockStatus,
				      droppedMemorySize: Long = 0L): Unit = {
				    val needReregister = !tryToReportBlockStatus(blockId, status, droppedMemorySize)
				    if (needReregister) {
				      logInfo(s"Got told to re-register updating block $blockId")
				      // Re-registering will report our new block for free.
				      asyncReregister()
				    }
				    logDebug(s"Told master about block $blockId")
				  }
				reportBlockStatus 用于向 BlockManagerMasterActor 报告 block的状态,并重新注册blockmanager
				首先调用 tryToReportBlockStatus, 发送消息更新block的情况,内存大小,磁盘大小, 存储级别
				if blockmanager没有向 BlockManagerMasterActor 注册, 调用 asyncReregister 来注册;
				updateBlockStatus, 
					def updateBlockInfo(
					    blockManagerId: BlockManagerId,
					    blockId: BlockId,
					    storageLevel: StorageLevel,
					    memSize: Long,
					    diskSize: Long): Boolean = {
					  val res = driverEndpoint.askSync[Boolean](
					    UpdateBlockInfo(blockManagerId, blockId, storageLevel, memSize, diskSize))
					  logDebug(s"Updated info of block $blockId")
					  res
					}
				调用了asksync方法

			3.8.3 单对象块的写入方法 putSingle
				def putSingle[T: ClassTag](
				    blockId: BlockId,
				    value: T,
				    level: StorageLevel,
				    tellMaster: Boolean = true): Boolean = {
				  putIterator(blockId, Iterator(value), level, tellMaster)
				}
				def putIterator[T: ClassTag](
				    blockId: BlockId,
				    values: Iterator[T],
				    level: StorageLevel,
				    tellMaster: Boolean = true): Boolean = {
				  require(values != null, "Values is null")
				  doPutIterator(blockId, () => values, level, implicitly[ClassTag[T]], tellMaster) match {
				    case None =>
				      true
				    case Some(iter) =>
				      // Caller doesn't care about the iterator values, so we can close the iterator here
				      // to free resources earlier
				      iter.close()
				      false
				  }
				}
				将单一对象构成的block写入存储系统,putsingle经过层层调用,实际调用了 doPutIterator 方法

			3.8.4 序列化字节快写入方法 putBytes
				用于将 序列化字节组成的 block写入存储系统;
				实际调用了doputbytes 方法

			3.8.5 doputbytes 方法
				书中的处理流程讲的很清楚了;见截图
				答题上,先判断是否存在blockinfo缓存;
				然后判断是否使用内存;
				然后判断是否使用Tachyon;
				然后判断是否使用磁盘;
				最后抛出异常.
			3.8.6 数据块备份方法 replicate
				BlockManager.scala 中 replicate方法,复制备份数据块
				maxReplicationFailures = 最大复制失败次数
				numPeersToReplicateTo = 需要复制的备份数
				peersForReplication = 可以作为备份的BlockManager的缓存
				peersReplicatedTo = 已经作为备份的BlockManager的缓存
				peersFailedToReplicateTo = 已经复制失败的BlockManager的缓存
				numFailures = 失败次数
				done = 复制是否完成

				为了容灾, peersForReplication 中缓存的BlockManager 不应该是当前的BlockManager;
				获取其他所有BlockManager的方法是 getPeers; spark.storage.BlockManager类
					private def getPeers(forceFetch: Boolean): Seq[BlockManagerId] = {
					  peerFetchLock.synchronized {
					    val cachedPeersTtl = conf.getInt("spark.storage.cachedPeersTtl", 60 * 1000) // milliseconds
					    val timeout = System.currentTimeMillis - lastPeerFetchTime > cachedPeersTtl
					    if (cachedPeers == null || forceFetch || timeout) {
					    // master = BlockManagerMaster
					      cachedPeers = master.getPeers(blockManagerId).sortBy(_.hashCode)
					      lastPeerFetchTime = System.currentTimeMillis
					      logDebug("Fetched peers from master: " + cachedPeers.mkString("[", ",", "]"))
					    }
					    cachedPeers
					  }
					}

					cachedPeersTtl = 当前BlockManager 缓存的BlockManagerId
					cachedPeers = 缓存的超时时间, 默认60s
					forceFetch = 是否强制从 BlockManagerMasterActor 获取最新的 BlockManagerId
				当 cachedPeers 为空或者forceFetch=true,或者当前时间超时了,则会调用 BlockManagerMaster的 getPeers 方法 从BlockManagerMasterActor 获取最新的 BlockManagerId
					def getPeers(blockManagerId: BlockManagerId): Seq[BlockManagerId] = {
					  driverEndpoint.askSync[Seq[BlockManagerId]](GetPeers(blockManagerId))
					}
				getPeers调用了askSync
				BlockManagerMasterEndpoint,替代了 BlockManagerMasterMaster;
				它的匹配 GetPeers 消息,将执行getPeer方法,从 BlockManagerInfo中,过滤掉 Driver的 BlockManager, 和当前的 BlockManager, 将其余的 BlockManagerId 返回

				replicate 方法,首先获取 BlockManagerId,并且保证再同一个节点上多次尝试复制同一个Block,保证它始终被复制到同一批节点上.
				当失败,再次尝试时,强制从BlockManagerMasterActor获取最新的BlockManagerId,并且从 peersForReplication 排除 peersReplicatedTo 和 peersFailedToReplicateTo 排除已经使用的和失败的Id

				复制的过程如下:
					获取 BlockManager
					上传 Block到BlockManager
					如果上传成功,将此BlockManager添加到peersReplicatedTo;而从peersForReplication中移除;设置 maxReplicationFailed = false,done = true
					如果上传出现异常,将此BlockManager添加到peersReplicateTo, failure自增,设置 replicationFailed = true,done = false
				如果上传失败,会迭代多次上述过程,直到失败次数>最大失败次数
				异步上传方法 uploadBlockSync,实际通过调用blockTransferService.uploadBlock来完成的
					val buffer = new BlockManagerManagedBuffer(blockInfoManager, blockId, data, false,
					          unlockOnDeallocate = false)
					blockTransferService.uploadBlockSync(

				updateBlockSync的实际代码如下
					def uploadBlockSync(
					    hostname: String,
					    port: Int,
					    execId: String,
					    blockId: BlockId,
					    blockData: ManagedBuffer,
					    level: StorageLevel,
					    classTag: ClassTag[_]): Unit = {
					  val future = uploadBlock(hostname, port, execId, blockId, blockData, level, classTag)
					  ThreadUtils.awaitResult(future, Duration.Inf)
					}
				3.8.7 创建 DiskBlockObjectWriter 的方法 getDiskWriter,获得磁盘的writer
					getDiskWriter 用于创建DiskBlockObjectWriter; spark.shuffle.sync决定写操作是同步还是异步
							def getDiskWriter(
								 blockId: BlockId,
								 file: File,
								 serializerInstance: SerializerInstance,
								 bufferSize: Int,
								 writeMetrics: ShuffleWriteMetrics): DiskBlockObjectWriter = {
								val syncWrites = conf.getBoolean("spark.shuffle.sync", false)
								new DiskBlockObjectWriter(file, serializerManager, serializerInstance, bufferSize,
									syncWrites, writeMetrics, blockId)
							}
				3.8.8 获取本地Block的数据方法 getBlockData
					从本地获取Block的数据
						override def getBlockData(blockId: BlockId): ManagedBuffer = {
							if (blockId.isShuffle) {
								shuffleManager.shuffleBlockResolver.getBlockData(blockId.asInstanceOf[ShuffleBlockId])
							} else {
								getLocalBytes(blockId) match {
									case Some(blockData) =>
										new BlockManagerManagedBuffer(blockInfoManager, blockId, blockData, true)
									case None =>
										// If this block manager receives a request for a block that it doesn't have then it's
										// likely that the master has outdated block statuses for this block. Therefore, we send
										// an RPC so that this block is marked as being unavailable from this block manager.
										reportBlockStatus(blockId, BlockStatus.empty)
										throw new BlockNotFoundException(blockId.toString)
								}
							}
						}
					如果 Block是shufflemaptask的输出,那么多个partition的中间结果都写入了同一个文件,怎么样兑取不同partition的中间结果?
					用ShuffleBlockResolver.getBlockData方法解决此问题
					如果 Block不是suffle的结果,是resulttask的结果,用getlocalbytes来获取本地中间结果数据
						如果是BlockData-> BlockManagerManagedBuffer
						如果是None -> reportBlockStatus,报告Block状态,报错

				3.8.9 获取本地shuffle数据方法 本地result,不是shufflemaptask,窄依赖的结果
					reduce与map属于同一节点,无需宽依赖,只要调用getlocalBytes即可
					而getLocalBytes
						如果blockid是shuffle的,用shuffleManager.shuffleBlockResolver.getBlockData来获取BlockData
						如果不是shuffle的,是窄依赖,最终通过doGetLocalBytes来获取本地Bytes
					而 doGetLocalBytes
						如果 Block允许使用内存,则调用 MemoryStore的getValues或者getBytes方法获取
						如果 Block允许使用DiskStore,则调用DiskStore的getBytes方法获取


					3.8.10 获取远程BLock的数据方法 doGetRemote->新版本的BlockManager.getRemoteBytes方法
						参数 runningFailureCount = 运行中失败次数
								 totalFailureCount = 全部失败次数
									locations = 位置
									maxFetchFailures = 允许最大失败次数
									locationIterator = 位置迭代器
						当location有next
							loc = 获取next的值
								blockTransferService.fetchBlockSync获取loc的内部值
							如果报错了怎么办?
								xxx

					3.8.11 获取Block数据方法get
						def get[T: ClassTag](blockId: BlockId): Option[BlockResult] = {
							val local = getLocalValues(blockId)
							if (local.isDefined) {
								logInfo(s"Found block $blockId locally")
								return local
							}
							val remote = getRemoteValues[T](blockId)
							if (remote.isDefined) {
								logInfo(s"Found block $blockId remotely")
								return remote
							}
							None
						}
						首先,尝试从本地获取,如果没有,才去远程获取

					3.8.12 数据流序列化方法 dataSerializeStream
						如果写入存储体系的数据本身就序列化了,那么读取时要反序列化
						dataSerializeStream,使用compressionCodec对文件输入流进行压缩和序列化处理
						spark.serializer.SerializerManager.dataSerializeStream
							def dataSerializeWithExplicitClassTag(
								 blockId: BlockId,
								 values: Iterator[_],
								 classTag: ClassTag[_]): ChunkedByteBuffer = {
								val bbos = new ChunkedByteBufferOutputStream(1024 * 1024 * 4, ByteBuffer.allocate)
								val byteStream = new BufferedOutputStream(bbos)
								val autoPick = !blockId.isInstanceOf[StreamBlockId]
								val ser = getSerializer(classTag, autoPick).newInstance()
								ser.serializeStream(wrapForCompression(blockId, byteStream)).writeAll(values).close()
								bbos.toChunkedByteBuffer
							}
					其中,wrapForCompression,spark.serializer.SerializerManager.dataSerializeStream下面的方法

				3.9 metadataCleaner broadcastCleaner
					为了有效利用磁盘空间和内存 metadataCleaner broadcastCleaner 分别清除 blockInfo(TimeStampedHashMap[BlockId, BlockInfo])中很久不用的非广播和广播Block信息
					metadataCleaner 的关键是 cleanupFunc:(Long)=>Unit,函数参数
					似乎被取消了
				3.10 缓存管理器 CacheManager spark.sql.execution.cacheManager
					用于缓存RDD某个分区计算后的中间结果
					RDD并非都缓存在CacheManager的存储部分中; CacheManager知识针对BlockManager的代理;真正的缓存依然使用BlockManager
					当判断存储级别使用了缓存,就会调用CacheManager的getorcompute方法
					executor端从内存缓存或者磁盘缓存读取RDD partition,如果没有,则更新RDD partition到内存缓存或者磁盘缓存

					spark.rdd.RDD.scala 中的 getOrCompute		