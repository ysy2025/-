5,计算引擎

Spark的计算式一个层层迭代的过程
RDD式对各种数据计算模型的统一抽象,被用来迭代计算过程和任务输出结果的缓存读写.
sdhuffle式连接map和reduce的桥梁.
shuffle的性能优劣,直接决定了计算引擎的性能和吞吐.

5.1 迭代计算
RDD.scala的iterator方法是迭代计算的根源
final def iterator(split: Partition, context: TaskContext): Iterator[T] = {
  if (storageLevel != StorageLevel.NONE) {
    getOrCompute(split, context)
  } else {
    computeOrReadCheckpoint(split, context)
  }
}
如果storage等级非none,说明有缓存,就调用 getOrCompute;传入split和context
反之,computeOrReadCheckpoint;要么计算,要么读取checkpoint里面的数据
getOrCompute在同一个RDD.scala文件中

computeOrReadCheckpoint的实现也是在RDD.scala中
private[spark] def computeOrReadCheckpoint(split: Partition, context: TaskContext): Iterator[T] =
{
  if (isCheckpointedAndMaterialized) {
    firstParent[T].iterator(split, context)
  } else {
    compute(split, context)
  }
}
如果是ckpt,而且已经materialize了,就利用iterator;找到其父RDD,调用iterator方法,其实就是上面的iterator方法

如果不是,计算context
这里原书说,查看线程栈更加直观,这个查看线程栈的方式我还没有找到...
跳过;在HadoopRDD中,看compute方法
首先,利用
	val iter = new NextIterator[(K, V)]
初始化 NextIterator;
1,从broadcast中获取jobconf;
2,创建InputMetrics,用来计算字节读取的测量信息;在recordreader正式读取数据之前创建bytereadcallback,用来获取当前线程从文件系统读取的字节数
3,获取input格式;
4,使用addlocalconfiguration,给jobconf添加hadoop任务相关配置
5,创建RecordReader
下面是定义的几个方法;先掠过
6,将 NextIterator封装为 InterruptibleIterator

rdd.terator调用结束后,会调用 SortShuffleWriter.scala的 write方法
1,创建 ExternalSorter;将计算结果写入缓存
2,调用 val output = shuffleBlockResolver.getDataFile(dep.shuffleId, mapId)
3,创建blockid val blockId = ShuffleBlockId(dep.shuffleId, mapId, IndexShuffleBlockResolver.NOOP_REDUCE_ID)
4,val partitionLengths = sorter.writePartitionedFile(blockId, tmp),将分区结果写入tmp中
5,shuffleBlockResolver.writeIndexFileAndCommit(dep.shuffleId, mapId, partitionLengths, tmp),创建索引
6,mapStatus = MapStatus(blockManager.shuffleServerId, partitionLengths),创建mapstatus


5.2 Shuffle
shuffle,是所有MR计算框架必经之路;shuffle用于打通map任务输出与reduce任务输入;map的输出结果按照key,hash之后,分配给一个reduce任务
早期的shuffle存在的问题:
1,map任务的中间结果首先存入内存然后写入磁盘;对内存开销要求很大.
2,每个map任务产生R个bucket(result任务数量);shuffle需要的bucket=M*R,在shuffle频繁的情况下,磁盘IO将成为性能瓶颈.

reduce任务获取map任务的中间输出时,需要在磁盘上merge sort;这产生了更多的磁盘IO
数据量很小时,map和reduce任务很多时,产生很多网络IO

目前Spark的优化策略:
1,map任务给每个partition的reduce任务输出的bucket合并到一个文件,避免磁盘IO消耗太大
2,map任务逐条输出结果,而不是一次性输出到内存中
3,磁盘+内存共同写,避免内存溢出
4,reduce任务,对于拉取到map任务中间结果逐条读取,而不是一次性读入内存,并在内存中聚合排序;避免占用大量数据
5,reduce任务将要拉取的block按照blockmanager地址划分,将同一个manager的block积累在一起成为少量网络请求,减少网络IO


5.3 map段计算结果缓存处理
两个概念:
bypassMergeThreshold:传递到reduce再做合并操作的阈值;如果partition数量小于该值,不用执行聚合和排序,直接将分区写到executor的存储文件中,最后在reduce端再做串联
bypassMergeSort:是否传递到reduce端再做合并排序;是否直接将各个partition直接写到executor的存储文件中.避免占用大量内存,内存溢出

map端计算结果缓存,3种方式:
map端对计算结果在缓存中聚合排序
map不适用缓存,不执行聚合排序,直接spilltopartitionfiles,将分区写到自己的存储文件,最后由reduce端对计算结果执行合并和排序
map端对计算结果简单缓存

在spark.util.collection.ExternalSorter.scala中,定义的 insertAll
根据我们是否需要combine来处理;一种,是内存优先存储,一种是磁盘优先存储
def insertAll(records: Iterator[Product2[K, V]]): Unit = {
  // TODO: stop combining if we find that the reduction factor isn't high
  val shouldCombine = aggregator.isDefined
  if (shouldCombine) {
    // Combine values in-memory first using our AppendOnlyMap
    val mergeValue = aggregator.get.mergeValue
    val createCombiner = aggregator.get.createCombiner
    var kv: Product2[K, V] = null
    val update = (hadValue: Boolean, oldValue: C) => {
      if (hadValue) mergeValue(oldValue, kv._2) else createCombiner(kv._2)
    }
    while (records.hasNext) {
      addElementsRead()
      kv = records.next()
      map.changeValue((getPartition(kv._1), kv._1), update)
      maybeSpillCollection(usingMap = true)
    }
  } else {
    // Stick values into our buffer
    while (records.hasNext) {
      addElementsRead()
      val kv = records.next()
      buffer.insert(getPartition(kv._1), kv._1, kv._2.asInstanceOf[C])
      maybeSpillCollection(usingMap = false)
    }
  }
}

任务分区很多时,如果将数据存到executor,在reduce中会存在大量网络IO,形成性能瓶颈.reduce读取map的结果变慢,导致其他想要分配到被这些map任务占用的节点的任务需要等待或者分配到更远的节点上;效率低下
在map端,就聚合排序,可以节省IO操作,提升系统性能;
因此需要定义聚合器aggregator函数,用来对结果聚合排序