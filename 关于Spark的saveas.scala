最近在工作中遇到一个问题:dataframe输出的时候,保存成text,saveAsTextFile;
但是,由于客户需求,需要将text直接保存成json格式.几经周折,终于完成.但是我突发奇想,想从源码中扒一扒,具体的原理
首先,是saveAsTextFile
apache.spark.rdd.RDD.scala中,包含了我们常见的RDD的API
主要成分就是一个RDD类

abstract class RDD
	这里注意,@transient private var _sc: SparkContext,无法序列化
	@transient private var deps: Seq[Dependency[_]],依赖也是无法序列化的
	同样,需要扩展serializable,允许序列化

	首先根据条件,提示报警,spark不支持nested RDDs,如果有就报警

	然后是定义SparkContext;如果缺少环境,就报错;

	紧接着一个构造函数 this
		利用context和依赖,完成构建 this(oneParent.context, List(new OneToOneDependency(oneParent)))
	定义conf函数,获取context的conf

	定义compute函数;结果是一个Iterator,迭代器
	
	定义getPartitions函数,获取分区,返回一个Array[Partition]

	定义getDependencies,获取依赖,返回值是Seq[Dependency]类型
	
	定义getPreferredLocations,获取更喜欢的位置;=deps,需要看依赖
	
	初始化,不可以序列化的 partitioner

	初始化 sparkContext;

	初始化 id
	
	定义setName函数,设置名称

	定义 persist函数,持久化,参数有 newLevel, allowOverride,允许覆盖?
		首先,对storagelevel,存储level进行处理;
		如果 storagelevel 非空, 而且 newlevel != storagelevel ,并且 allowOverride=true
			说明已经有storagelevel了,会报错
		如果是首次持久化-> 如果 storagelevel为空
			首先调用 sc.clean,清除
			然后sc.persistRDD

		剩下的,就是其他情况以及上述处理完后的操作:storagelevel不为空,但是 newlevel = storagelevel 或者 allowOverride!=true
		设置 storageLevel = newLevel
    	返回 this

    定义重载函数 persisit,只有 newLevel
    	此时,检查 isLocallyCheckpointed,如果 isLocallyCheckpointed = true,本地进行了checkpointed处理,就是已经持久化了
    		持久化persist(LocalRDDCheckpointData.transformStorageLevel(newLevel), allowOverride = true)
    	反之,本地没有ckpt,那么不能override
    		persist(newLevel, allowOverride = false)

    定义重载函数 persist,没有参数;this.type = persist(StorageLevel.MEMORY_ONLY)

    定义空函数 cache,缓存;this.type = persist()

    定义空函数 unpersist,解除持久化
    	Log记录
    	sc.unpersistRDD->SparkContext.unpersistRDD
    		首先master节点 removeRdd,移除RDD
    		然后持久化的RDDs移除该RddId
    		向listenerbus提交接触持久化信息
		然后设置 storageLevel = None,说明已经没有持久化了
		返回值为this

	定义 getStorageLevel,获取 storagelevel

	/**
	对于RDD的所有可变状态的锁.我们不用 `this`,因为RDDs对user来说是可以看到的,因此用户可能会增加自己的锁;这样会导致死锁现象
	对于一系列的RDD依赖而言,一个线程可能会持有多个锁,但是DAG是非循环的,在一个DAG中只有1个锁,不会出现死锁现象
	用整数,只是为了序列化而已.executor可能会引用
	**/
	定义状态锁 stageLock; = new Integer(0)

	/**
	我们的依赖,和分区,会被子类调用方法时获取到;同时也会在checkpointed的时候被覆盖
	**/
	初始化 变量 dependencies,依赖;Seq[Dependency[_]]类型;

	初始化 变量 partitions_, 分区, Array[Partition] = null;不可以序列化的

	/**
	checkpointRDD,用来持有checkpointRDD,如果我们被 checkpointed了的话
	**/
	定义 checkpointRDD方法

	/**
	获取RDD的一系列依赖;需要考虑是否checkpointed了,是否持久化了
	**/
	定义 dependencies方法, Seq[Dependency[_]]类
	/**
	getOrElse()主要就是防范措施,如果有值,那就可以得到这个值,如果没有就会得到一个默认值
	**/
		checkpointRDD映射,形成依赖List
		然后获取依赖

	/**
	获取RDD的分区list;考虑到是否持久化了.
	**/
	定义函数 partitions
		checkpointRDD映射;
		获取 partition

	@从1.6.0版本开始,
	定义 getNumPartitions = partitions.length,获取分区数量

	/**
	获取partition更倾向的位置;考虑到是否持久化了
	**/
	定义 getpreferredLocation
		对checkpointRDD,首先map,调用getPreferredLocations;然后调用getOrElse
	
	/**
	内部方法;从缓存中读取或者计算
	这些方法,不应该被客服直接调用,而是子类在实现的时候被调用
	**/
	定义 iterator(split: Partition, context: TaskContext),给定分区和context
		如果storageLevel非空,说明已经有storagelevel了,没有计算
			getOrCompute
		反之,说明没有持久化
			computeOrReadCheckpoint,直接计算或者读取ckpt

	/**
	返回RDD的祖先,通过narrowdependencies联系的祖先.这个遍历了给定的RDD的依赖树,也不给依赖排序
	**/
	定义 getNarrowAncestors,获取narrowancestors;
		初始化ancestors = HashSet
		定义visit方法;
			定义 narrowDependencies,窄依赖;
			定义 narrowParents,窄依赖的父系;
			定义 narrowParentsNotVisited,窄依赖的没有被访问的父系;
			对于没有访问的父系,ancestor.add它
			visit.add它,也就是访问过了
		visit(this),所有的都访问了
		//为了避免是cycle,不要把根包括在内

	/**
	定义 计算或者读取ckpt方法;
	**/
	定义 computeOrReadCheckpoint
		如果ckpt了
			父系.iterator(split, context);
		如果没有ckpt
			compute(split, context);

	/**
	定义 get或者计算分区
	**/
	定义 getOrCompute(partition: Partition, context: TaskContext)
		首先获取block的id
		给定flag,readCachedBlock,是否读取缓存的块
		调用 SparkEnv.get.blockManager.getOrElseUpdate,确定是get还是update
			如果是Left
				如果读取了缓存的块:
					获取存在的metrics;
					读取blockresult的bytes
					初始化新的 InterruptibleIterator,迭代器;
						定义next方法,读取1个record,然后delegate移动到下一个
				如果还没有读取缓存的块:
					始化新的 InterruptibleIterator,迭代器;但是返回类型和前面提到的不一样
			如果是Right
				初始化新的 InterruptibleIterator,参数不同,返回类型不同

	定义 withScope方法:
		U = RDDOperationScope.withScope[U](sc)(body)
		调用 RDDOperationScope 类的withScope方法