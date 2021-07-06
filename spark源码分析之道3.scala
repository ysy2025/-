4,任务提交与执行
任务的提交与执行,构建在存储体系与计算引擎之上.存储体系在第3章介绍了,计算引擎将在第5章介绍.

4.1 任务概述
4个步骤.
1,建立operator DAG,有向无环图;主要负责完成RDD的转换和DAG的构建
2,split graph into stages of tasks.此阶段,主要完成 finalStage 的创建和 Stage的划分;做好Stage与Task的准备工作后,提交stage和task
3,launch task via cluster manager;使用集群管理器,cluster manager, 分配资源与任务调度.对于失败的任务,还有一定的重试和容错机制
4,execute tasks,执行任务,将任务中间结果和最终结果存入存储体系.

JavaWordCount.java案例
public final class JavaWordCount {
  private static final Pattern SPACE = Pattern.compile(" ");

  public static void main(String[] args) throws Exception {

    if (args.length < 1) {
      System.err.println("Usage: JavaWordCount <file>");
      System.exit(1);
    }

    SparkSession spark = SparkSession
      .builder()
      .appName("JavaWordCount")
      .getOrCreate();

    JavaRDD<String> lines = spark.read().textFile(args[0]).javaRDD();

    JavaRDD<String> words = lines.flatMap(s -> Arrays.asList(SPACE.split(s)).iterator());

    JavaPairRDD<String, Integer> ones = words.mapToPair(s -> new Tuple2<>(s, 1));

    JavaPairRDD<String, Integer> counts = ones.reduceByKey((i1, i2) -> i1 + i2);

    List<Tuple2<String, Integer>> output = counts.collect();
    for (Tuple2<?,?> tuple : output) {
      System.out.println(tuple._1() + ": " + tuple._2());
    }
    spark.stop();
  }
}

hadoopFIle,构建HadoopRDD;
1,将hadoop的configuration封装为SerializableWritable,用于序列化读写操作;然后广播Hadoop的Configuration.通常大小只有10K,不会影响性能
2,定义偏函数,(jobConf:JobConf) => FileInputFormat.setInputPaths(jobConf,path), 用于设置输入路径
3,构建HadoopRDD
def hadoopFile[K, V](
    path: String,
    inputFormatClass: Class[_ <: InputFormat[K, V]],
    keyClass: Class[K],
    valueClass: Class[V],
    minPartitions: Int = defaultMinPartitions): RDD[(K, V)] = withScope {
  assertNotStopped()
  // This is a hack to enforce loading hdfs-site.xml.
  // See SPARK-11227 for details.
  FileSystem.getLocal(hadoopConfiguration)
  // A Hadoop configuration can be about 10 KB, which is pretty big, so broadcast it.
  val confBroadcast = broadcast(new SerializableConfiguration(hadoopConfiguration))
  val setInputPathsFunc = (jobConf: JobConf) => FileInputFormat.setInputPaths(jobConf, path)
  new HadoopRDD(
    this,
    confBroadcast,
    Some(setInputPathsFunc),
    inputFormatClass,
    keyClass,
    valueClass,
    minPartitions).setName(path)
}

4.2 广播Hadoop配置信息
SparkContext,的broadcast方法用于广播Hadoop的配置信息
def broadcast[T: ClassTag](value: T): Broadcast[T] = {
  assertNotStopped()
  require(!classOf[RDD[_]].isAssignableFrom(classTag[T].runtimeClass),
    "Can not directly broadcast RDDs; instead, call collect() and broadcast the result.")
  val bc = env.broadcastManager.newBroadcast[T](value, isLocal)
  val callSite = getCallSite
  logInfo("Created broadcast " + bc.id + " from " + callSite.shortForm)
  cleaner.foreach(_.registerBroadcastForCleanup(bc))
  bc
}
BroadcastManager,发送广播,广播结束后,将广播对象注册到ContextCleanner中
def newBroadcast[T: ClassTag](value_ : T, isLocal: Boolean): Broadcast[T] = {
  broadcastFactory.newBroadcast[T](value_, isLocal, nextBroadcastId.getAndIncrement())
}
newBroadcast方法中,是通过broadcastFactory.newBroadcast注册的
而broadcastFactory = new TorrentBroadcastFactory;
所以 broadcastFactory.newBroadcast = new TorrentBroadcastFactory.newBroadcast

TorrentBroadcastFactory.newBroadcast 方法实现如下
override def newBroadcast[T: ClassTag](value_ : T, isLocal: Boolean, id: Long): Broadcast[T] = {
  new TorrentBroadcast[T](value_, id)
}
这里的TorrentBroadcast,在TorrentBroadcast.scala中实现如下
private[spark] class TorrentBroadcast[T: ClassTag](obj: T, id: Long)
  extends Broadcast[T](id) with Logging with Serializable {

  /**
   * Value of the broadcast object on executors. This is reconstructed by [[readBroadcastBlock]],
   * which builds this value by reading blocks from the driver and/or other executors.
   *
   * On the driver, if the value is required, it is read lazily from the block manager.
   */
  @transient private lazy val _value: T = readBroadcastBlock()

  /** The compression codec to use, or None if compression is disabled */
  @transient private var compressionCodec: Option[CompressionCodec] = _
  /** Size of each block. Default value is 4MB.  This value is only read by the broadcaster. */
  @transient private var blockSize: Int = _

  private def setConf(conf: SparkConf) {
    compressionCodec = if (conf.getBoolean("spark.broadcast.compress", true)) {
      Some(CompressionCodec.createCodec(conf))
    } else {
      None
    }
    // Note: use getSizeAsKb (not bytes) to maintain compatibility if no units are provided
    blockSize = conf.getSizeAsKb("spark.broadcast.blockSize", "4m").toInt * 1024
    checksumEnabled = conf.getBoolean("spark.broadcast.checksum", true)
  }
  setConf(SparkEnv.get.conf)

  private val broadcastId = BroadcastBlockId(id)
  ...

块的写操作,writeBlocks
TorrentBroadcast.writeBlocks
1,将要写入的对象在本地的存储体系中备份一份,让task也可以在本地driver上运行
2,给bytearraychunckoutputstream指定压缩算法,并且将对象以序列化方式写入bytearraychunkoutputstream,然后转换为Array[ByteBuffer]
3,将每一个ByteBuffer作为一个Block使用putBytes方法写入存储体系

private def writeBlocks(value: T): Int = {
  import StorageLevel._
  //Store a copy of the broadcast variable in the driver so that tasks run on the driver do not create a duplicate copy of the broadcast variable's value.
  //将要写入的对象在本地的存储体系中备份一份,让task也可以在本地driver上运行
  val blockManager = SparkEnv.get.blockManager
  if (!blockManager.putSingle(broadcastId, value, MEMORY_AND_DISK, tellMaster = false)) {
    throw new SparkException(s"Failed to store $broadcastId in BlockManager")
  }
  // 给bytearraychunckoutputstream指定压缩算法
  val blocks =
    TorrentBroadcast.blockifyObject(value, blockSize, SparkEnv.get.serializer, compressionCodec)
  if (checksumEnabled) {
    checksums = new Array[Int](blocks.length)
  }
  //将每一个ByteBuffer作为一个Block使用putBytes方法写入存储体系
  blocks.zipWithIndex.foreach { case (block, i) =>
    if (checksumEnabled) {
      checksums(i) = calcChecksum(block)
    }
    val pieceId = BroadcastBlockId(id, "piece" + i)
    val bytes = new ChunkedByteBuffer(block.duplicate())
    if (!blockManager.putBytes(pieceId, bytes, MEMORY_AND_DISK_SER, tellMaster = true)) {
      throw new SparkException(s"Failed to store $pieceId of $broadcastId in local BlockManager")
    }
  }
  blocks.length
}
注意其中的val blocks = TorrentBroadcast.blockifyObject(value, blockSize, SparkEnv.get.serializer, compressionCodec)
这里,TorrentBroadcast.blockifyObject,用来将对象序列化(serializer),利用compressioncodec指定的压缩算法压缩,将value压缩成Array[ByteBuffer]
def blockifyObject[T: ClassTag](
    obj: T,
    blockSize: Int,
    serializer: Serializer,
    compressionCodec: Option[CompressionCodec]): Array[ByteBuffer] = {
  val cbbos = new ChunkedByteBufferOutputStream(blockSize, ByteBuffer.allocate)
  val out = compressionCodec.map(c => c.compressedOutputStream(cbbos)).getOrElse(cbbos)
  val ser = serializer.newInstance()
  val serOut = ser.serializeStream(out)
  Utils.tryWithSafeFinally {
    serOut.writeObject[T](obj)
  } {
    serOut.close()
  }
  cbbos.toChunkedByteBuffer.getChunks()
}

4.3 RDD转化 DAG构建
4.3.1 为啥需要RDD
1,处理模型
RDD是一个容错的,并行的数据结构;可以控制将数据存储到磁盘或者内存,能够获取数据的分区.RDD有两种操作,转换(transformation),计算(action)
通常数据处理模型:迭代计算,关系查询,mapreduce,流式处理等;场景有,流式计算,图计算,机器学习等
hadoop采用MR,Storm采用流式计算,Spark实现了以上所有

2,依赖划分原则
一个RDD包含一个或者多个分区(每个分区实际上是一个数据集合的片段)
构建DAG的过程中,RDD通过依赖关系串联起来.每个RDD都有依赖.分为NarrowDependency(窄依赖,不会涉及shuffle的transform)
SuffleDependency(宽依赖,涉及shuffle的transform),需要跨管道,跨节点传输数据.
容灾角度,NarrowDependency 只要计算父RDD的丢失分区即可; ShuffleDependency 需要考虑恢复所有父RDD的丢失分区

3,数据处理效率
ShuffleDependency, 依赖的上游RDD的计算过程允许在多个节点并发执行.ShuffleMapTask在多个节点的多个实例
如果数据很多,可以适当增加分区数量.这种根据硬件条件对并发任务数量的控制,能更好利用资源,提高Spark的数据处理效率

4,容错处理
传统关系型数据库利用日志记录来容灾容错,数据恢复依赖重新执行日志中的SQL
Hadoop通过数据备份到其他机器容灾
RDD本身无法变化,当某个Worker节点任务失败,可以利用DAG重新调度计算失败的任务;不用复制数据,降低了网络通信.
流式计算中,Spark需要记录日志和检查的,以便利用checkpoint和日志对数据恢复

4.3.2 RDD实现分析
hadoopFile的实现,最后实际上实例化了一个HadoopRDD
hadoopFile方法创建完HadoopRDD后,调用RDD的map方法.
def textFile(
    path: String,
    minPartitions: Int = defaultMinPartitions): RDD[String] = withScope {
  assertNotStopped()
  hadoopFile(path, classOf[TextInputFormat], classOf[LongWritable], classOf[Text],
    minPartitions).map(pair => pair._2.toString).setName(path)
}

map 方法将 HadoopRDD 封装为 MapPartitionsRDD;这里是RDD.scala模块下面的
def map[U: ClassTag](f: T => U): RDD[U] = withScope {
  val cleanF = sc.clean(f)
  new MapPartitionsRDD[U, T](this, (context, pid, iter) => iter.map(cleanF))
}

SparkContext clean方法
private[spark] def clean[F <: AnyRef](f: F, checkSerializable: Boolean = true): F = {
  ClosureCleaner.clean(f, checkSerializable)
  f
}
这里是为了清除闭包中的不能序列化的变量,防止RDD在网络传输过程中反序列化失败

构建MapPartitionsRDD的步骤如下

1,SparkContext.textFile 的map方法-> RDD.map方法的 MapPartitionsRDD -> MapPartitionsRDD类,扩展了RDD类-> RDD 类的 def this,
def this(@transient oneParent: RDD[_]) =
  this(oneParent.context, List(new OneToOneDependency(oneParent)))
封装为 OneToOneDependency,一对一依赖,继承自 NarrowDependency
class OneToOneDependency[T](rdd: RDD[T]) extends NarrowDependency[T](rdd) {
  override def getParents(partitionId: Int): List[Int] = List(partitionId)
}
2,调用RDD的主构造器.
abstract class RDD[T: ClassTag](
    @transient private var _sc: SparkContext,
    @transient private var deps: Seq[Dependency[_]]
  ) extends Serializable with Logging {

  protected def getDependencies: Seq[Dependency[_]] = deps

MapPartitionsRDD 会被隐式转化为JavaRDD.然后执行JavaRDD的flatMap方法.由于JavaRDD实现了JavaRDDLike特质,所以实际调用了JavaRDDLike的flatMap方法
利用JavaRDD的flatMap方法.这里,由于JavaRDD实现了JavaRDDLike特质,所以实际上调用了JavaRDDLike的flatMap方法
def flatMap[U](f: FlatMapFunction[T, U]): JavaRDD[U] = {
  def fn: (T) => Iterator[U] = (x: T) => f.call(x).asScala
  JavaRDD.fromRDD(rdd.flatMap(fn)(fakeClassTag[U]))(fakeClassTag[U])
}
此时,JavaRDD内部的rdd属性,实质上还是 MapPartitionsRDD,调用 MapPartitionsRDD 的flatMap方法
def flatMap[U: ClassTag](f: T => TraversableOnce[U]): RDD[U] = withScope {
  val cleanF = sc.clean(f)
  new MapPartitionsRDD[U, T](this, (context, pid, iter) => iter.flatMap(cleanF))
}
将 MapPartitionsRDD 继续封装成 MapPartitionsRDD

上面调用的JavaRDD.fromRDD方法实现如下
object JavaRDD {
  implicit def fromRDD[T: ClassTag](rdd: RDD[T]): JavaRDD[T] = new JavaRDD[T](rdd)
  implicit def toRDD[T](rdd: JavaRDD[T]): RDD[T] = rdd.rdd
}

执行 JavaWordCount.java 中
JavaPairRDD<String, Integer> ones = words.mapToPair(s -> new Tuple2<>(s, 1));
这里的maptoPairs方法实现如下
def mapToPair[K2, V2](f: PairFunction[T, K2, V2]): JavaPairRDD[K2, V2] = {
  def cm: ClassTag[(K2, V2)] = implicitly[ClassTag[(K2, V2)]]
  new JavaPairRDD(rdd.map[(K2, V2)](f)(cm))(fakeClassTag[K2], fakeClassTag[V2])
}

此时JavaRDD内部的rdd属性还是 MapPartitionsRDD;调用RDD的map方法,继续封装成
def map[U: ClassTag](f: T => U): RDD[U] = withScope {
  val cleanF = sc.clean(f)
  new MapPartitionsRDD[U, T](this, (context, pid, iter) => iter.map(cleanF))
}
MapPartitionsRDD

继续执行 JavaPairRDD<String, Integer> counts = ones.reduceByKey((i1, i2) -> i1 + i2);
这里的 reduceByKey, 实现方法如下
def reduceByKey(func: JFunction2[V, V, V]): JavaPairRDD[K, V] = {
  fromRDD(reduceByKey(defaultPartitioner(rdd), func))
}
这里的 defaultPartitioner 在Partitioner.scala模块下面的
def defaultPartitioner(rdd: RDD[_], others: RDD[_]*): Partitioner = {
  val rdds = (Seq(rdd) ++ others)
  val hasPartitioner = rdds.filter(_.partitioner.exists(_.numPartitions > 0))
  if (hasPartitioner.nonEmpty) {
    hasPartitioner.maxBy(_.partitions.length).partitioner.get
  } else {
    if (rdd.context.conf.contains("spark.default.parallelism")) {
      new HashPartitioner(rdd.context.defaultParallelism)
    } else {
      new HashPartitioner(rdds.map(_.partitions.length).max)
    }
  }
}
首先,将RDD转换为Seq,然后对Seq排序
然后常见 HashPartitioner 对象, 如果配置了spark.default.parallelism,用它做分区数量

这里的 hasPartitioner.maxBy(_.partitions.length).partitioner.get
partitions方法实现如下 RDD.scala
final def partitions: Array[Partition] = {
  checkpointRDD.map(_.partitions).getOrElse {
    if (partitions_ == null) {
      partitions_ = getPartitions
      partitions_.zipWithIndex.foreach { case (partition, index) =>
        require(partition.index == index,
          s"partitions($index).partition == ${partition.index}, but it should equal $index")
      }
    }
    partitions_
  }
}

这里的getpartitions方法
RDD.scala
protected def getPartitions: Array[Partition]

老版本里面有 firstParent方法,新版本没有在这里调用.不过也还是在这里展示一下
protected[spark] def firstParent[U: ClassTag]: RDD[U] = {
  dependencies.head.rdd.asInstanceOf[RDD[U]]
}
这里,firstParent,用于返回依赖的第一个父RDD.

这里面一堆引用链条,最终,partitions调用的是HadoopRDD.scala里面的getPartitions方法
override def getPartitions: Array[Partition] = {
  val jobConf = getJobConf()
  // add the credentials here as this can be called before SparkContext initialized
  SparkHadoopUtil.get.addCredentials(jobConf)
  val inputFormat = getInputFormat(jobConf)
  val inputSplits = inputFormat.getSplits(jobConf, minPartitions)
  val array = new Array[Partition](inputSplits.size)
  for (i <- 0 until inputSplits.size) {
    array(i) = new HadoopPartition(id, i, inputSplits(i))
  }
  array
}

最后,JavaPairRDD<String, Integer> counts = ones.reduceByKey((i1, i2) -> i1 + i2);
这里,reduceByKey的实现
def reduceByKey(func: JFunction2[V, V, V]): JavaPairRDD[K, V] = {
  fromRDD(reduceByKey(defaultPartitioner(rdd), func))
}

最终是调用PairRDDFunctions.scala里面的 reduceByKey方法
按照书中的说法,这里发生了隐式转化;确实存在
implicit def rddToPairRDDFunctions[K, V](rdd: RDD[(K, V)])
  (implicit kt: ClassTag[K], vt: ClassTag[V], ord: Ordering[K] = null): PairRDDFunctions[K, V] = {
  new PairRDDFunctions(rdd)
}

经过多次转换,可以用PairRDDFunctions的reduceByKey方法
def reduceByKey(partitioner: Partitioner, func: (V, V) => V): RDD[(K, V)] = self.withScope {
  combineByKeyWithClassTag[V]((v: V) => v, func, func, partitioner)
}

这里的 combineByKeyWithClassTag, PairRDDFunctions.scala 中combineByKeyWithClassTag方法
def combineByKeyWithClassTag[C](
    createCombiner: V => C,
    mergeValue: (C, V) => C,
    mergeCombiners: (C, C) => C,
    partitioner: Partitioner,
    mapSideCombine: Boolean = true,
    serializer: Serializer = null)(implicit ct: ClassTag[C]): RDD[(K, C)] = self.withScope {
  require(mergeCombiners != null, "mergeCombiners must be defined") // required as of Spark 0.9.0
  if (keyClass.isArray) {
    if (mapSideCombine) {
      throw new SparkException("Cannot use map-side combining with array keys.")
    }
    if (partitioner.isInstanceOf[HashPartitioner]) {
      throw new SparkException("HashPartitioner cannot partition array keys.")
    }
  }
  val aggregator = new Aggregator[K, V, C](
    self.context.clean(createCombiner),
    self.context.clean(mergeValue),
    self.context.clean(mergeCombiners))
  if (self.partitioner == Some(partitioner)) {
    self.mapPartitions(iter => {
      val context = TaskContext.get()
      new InterruptibleIterator(context, aggregator.combineValuesByKey(iter, context))
    }, preservesPartitioning = true)
  } else {
    new ShuffledRDD[K, V, C](self, partitioner)
      .setSerializer(serializer)
      .setAggregator(aggregator)
      .setMapSideCombine(mapSideCombine)
  }
}
开始看看有没有问题.有问题报警,没有问题继续
首先创建Aggregator
然后,根据self.partitioner != Some(partitioner)条件来创建 ShuffledRDD
ShuffledRDD.scala中包含了 ShuffledRDD 的实现方法

在 reduceByKey方法的实现中,fromRDD方法,将shuffleRDD重新封装成为JavaPairRDD
def fromRDD[K: ClassTag, V: ClassTag](rdd: RDD[(K, V)]): JavaPairRDD[K, V] = {
  new JavaPairRDD[K, V](rdd)
}

4.4 任务提交
4.4.1 任务提价的准备
接下来要执行 JavaPairRDD的word count 方法了. collect方法,调用了RDD的collect方法后转成Seq,并封装Seq成为ArrayList.
新版本在JavaRDDLike.scala中
def collect(): JList[T] =
  rdd.collect().toSeq.asJava
老版本中
def collect():JList[T] = {
	import scala.collection.JavaConversions._
	val arr:java.util.Collection[T] = rdd.collect().toSeq
	new java.util.ArrayList(arr)
}

RDD的collect方法实现方式如下,在RDD.scala中
def collect(): Array[T] = withScope {
  val results = sc.runJob(this, (iter: Iterator[T]) => iter.toArray)
  Array.concat(results: _*)
}

SparkContext的runjob调用了重载的runJob
这里的runJob调用SparkContext.scala
def runJob[T, U: ClassTag](rdd: RDD[T], func: Iterator[T] => U): Array[U] = {
  runJob(rdd, func, 0 until rdd.partitions.length)
}
这里的runJob再次重载,都是往上
def runJob[T, U: ClassTag](
    rdd: RDD[T],`
    func: Iterator[T] => U,
    partitions: Seq[Int]): Array[U] = {
  val cleanedFunc = clean(func)
  runJob(rdd, (ctx: TaskContext, it: Iterator[T]) => cleanedFunc(it), partitions)
}
继续上溯
def runJob[T, U: ClassTag](
    rdd: RDD[T],
    func: (TaskContext, Iterator[T]) => U,
    partitions: Seq[Int]): Array[U] = {
  val results = new Array[U](partitions.size)
  runJob[T, U](rdd, func, partitions, (index, res) => results(index) = res)
  results
}

最后上溯到
def runJob[T, U: ClassTag](
    rdd: RDD[T],
    func: (TaskContext, Iterator[T]) => U,
    partitions: Seq[Int],
    resultHandler: (Int, U) => Unit): Unit = {
  if (stopped.get()) {
    throw new IllegalStateException("SparkContext has been shutdown")
  }
  val callSite = getCallSite
  val cleanedFunc = clean(func)
  logInfo("Starting job: " + callSite.shortForm)
  if (conf.getBoolean("spark.logLineage", false)) {
    logInfo("RDD's recursive dependencies:\n" + rdd.toDebugString)
  }
  dagScheduler.runJob(rdd, cleanedFunc, partitions, callSite, resultHandler, localProperties.get)
  progressBar.foreach(_.finishAll())
  rdd.doCheckpoint()
}
调用clean方法,纺织闭包的反序列化错误;并且运行dagScheduler的runJob;DAGScheduler.scala 的runJob
这里主要调用submitJob,提交任务.
waiter.awaitResult说明任务是异步的



1,提交任务