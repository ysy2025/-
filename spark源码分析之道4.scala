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