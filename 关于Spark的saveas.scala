最近在工作中遇到一个问题:dataframe输出的时候,保存成text,saveAsTextFile;
但是,由于客户需求,需要将text直接保存成json格式.几经周折,终于完成.但是我突发奇想,想从源码中扒一扒,具体的原理
首先,是saveAsTextFile
apache.spark.rdd.RDD.scala中,包含了我们常见的RDD的API
主要成分就是一个RDD类

abstract class RDD
	这里注意,@transient private var _sc: SparkContext,无法序列化
	@transient private var deps: Seq[Dependency[_]],依赖也是无法序列化的
	同样,需要扩展serializable,允许序列化

	首先根据条件,输出,spark不支持nested RDDs

	然后是定义SparkContext;如果缺少环境,就报错;

	紧接着一个构造函数 this
		利用context和依赖,完成构建
	定义conf函数,获取context的conf

	定义compute函数;结果是一个Iterator,迭代器
	定义getPartitions函数,获取分区,返回一个Array[Partition]

	定义getDependencies,获取依赖,返回值是Seq[Dependency]类型
	定义getPreferredLocations,获取更喜欢的位置
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
    	此时,检查 isLocallyCheckpointed,如果 isLocallyCheckpointed = true,本地进行了checkpointed处理
    		持久化persist(LocalRDDCheckpointData.transformStorageLevel(newLevel), allowOverride = true)
    	反之:
    		persist(newLevel, allowOverride = false)

    定义重载函数 persist,没有参数

    定义空函数 cache,缓存;

    定义空函数 unpersist,解除持久化
    	Log记录
    	sc.unpersistRDD->SparkContext.unpersistRDD
		然后设置 storageLevel
		返回值为this

	定义 getStorageLevel,获取 storagelevel
	初始化 变量 dependencies,依赖;Seq[Dependency[_]]类型;
	初始化 变量 partitions_, 分区, Array[Partition] = null;不可以序列化的

	定义 checkpointRDD方法

	定义 dependencies方法, Seq[Dependency[_]]类
	//getOrElse()主要就是防范措施,如果有值,那就可以得到这个值,如果没有就会得到一个默认值
		checkpointRDD映射,形成依赖List
			然后获取依赖

	定义函数 partitions
		checkpointRDD映射;
			获取 partition

	