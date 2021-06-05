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

			2.2.5 获取线程shuffle文件
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

			2.2.6 上传 shuffle文件
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

		2.3 BlockManagerMaster 对于 BlockManager的管理
			Driver上的 BlockManagerMaster, 对于存在Executor上面的BlockManager,统一管理
			但是如果 Driver和Executor 位于不同机器时,如何是好?
			Driver的BlockManagerMaster 会持有 BlockManagerMasterActor, 所有executor也会从ActorSystem中获取 BlockManagerMasterActor的引用
			所有 Executor 和Driver 关于BlockManager的交互都依赖它
			这里的 BlockManagerMasterActor 实际上应该是 Rpc了;Actor已经被取消了

			2.3.1 BlockManagerMasterActor
				BlockManagerMasterActor 只存在于Driver上;Executor从ActorSystem获取BlockManagerMasterActor的引用;
				然后给BlockManagerMasterActor发送消息,实现和Driver的交互
				新版本中,BlockManagerMasterActor被 BlockManagerMasterEndpoint替代
				BlockManagerMasterEndpoint,维护了很多缓存数据结构
				blockManagerInfo, blockmanagerid和blockmanager的信息
				blockmanageridbyexecutor, 缓存executorid与其拥有的blockmanagerid之间的映射关系
				blocklocation,缓存block与blockmanagerid的映射关系
				

			2.3.2 询问Driver并获取回复方法
				在Executor的BlockManagerMaster中,所有与Driver上BlockManagerMaster的交互方法最终都调用了askDriverwithReply

			2.3.3 向Blockmanagermaster注册blockmanagerid
				executor或者driver,本身的blockmanager在初始化时,需要向driver的blockmanager注册blockmanager信息

		2.4 磁盘块管理器
			2.4.1 DiskBlockManager的构造过程
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

			2.4.2 获取磁盘文件方法 getFile
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
			2.4.3 创建临时 Block 方法,createTempShuffleBlock
				当shuffleMapTask运行结束时,需要把中间结果临时保存;此时,调用 createTempShuffleBlock,作为临时Block,返回TempShuffleBlockId与其文件的对偶
					def createTempShuffleBlock(): (TempShuffleBlockId, File) = {
					  var blockId = new TempShuffleBlockId(UUID.randomUUID())
					  while (getFile(blockId).exists()) {
					    blockId = new TempShuffleBlockId(UUID.randomUUID())
					  }
					  (blockId, getFile(blockId))
					}

		2.5 磁盘存储DiskStore
			当MemoryStore没有足够空间,会使用DiskStore,存入磁盘;DiskStore集成BlockStore,实现了getBytes,putBytes,putArray,putIterator等