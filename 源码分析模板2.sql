ListenerBus
ListenerBus[L <: AnyRef, E]是事件总线的基础特质,支持任何类型的监听器,E表示事件类型,接受事件并且将事件提交到对应事件的监听器.

主要属性如下

listeners, listenersPlusTimers：维护了所有的监听器和对应的定时器,数据结构为线程安全的CopyOnWriteArrayList适用于读多写少的业务场景,满足数据的最终一致性

private[this] val listenersPlusTimers = new CopyOnWriteArrayList[(L, Option[Timer])]
  
// Marked `private[spark]` for access in tests.
private[spark] def listeners = listenersPlusTimers.asScala.map(_._1).asJava
主要方法如下

addListener(), removeListener()：从listenersPlusTimers中增加或者删除监听器和计时器
postToAll()：遍历listenersPlusTimers并调用未实现的doPostEvent()方法发送事件
如图所示,每个实现类实现了doPostEvent方法,利用模式匹配将特定的事件投递到对应的监视器类型.



以SparkListenerBus为例,实现的doPostEvent方法用于将继承自SparkListenerEvent的样例类事件投递到继承自SparkListenerInterface的监听器中.SparkListenerInterface监视器定义了未实现的回调函数,用于处理对应的事件.

// 监听器
private[spark] trait SparkListenerInterface {

  /**
   * Called when a stage completes successfully or fails, with information on the completed stage.
   */
  def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit
  
  ...
}

// 事件
@DeveloperApi
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "Event")
trait SparkListenerEvent {
  /* Whether output this event to the event log */
  protected[spark] def logEvent: Boolean = true
}
...
@DeveloperApi
case class SparkListenerStageCompleted(stageInfo: StageInfo) extends SparkListenerEvent
...

// 事件总线
private[spark] trait SparkListenerBus
extends ListenerBus[SparkListenerInterface, SparkListenerEvent] {

  protected override def doPostEvent(
    listener: SparkListenerInterface,
    event: SparkListenerEvent): Unit = {
    event match {
      case stageSubmitted: SparkListenerStageSubmitted =>
      listener.onStageSubmitted(stageSubmitted)
      ...
    }
  }
}
AsyncEventQueue
AsyncEventQueue继承自SparkListenerBus是事件的异步队列,事件的分发都将分配独立的线程,防止在监听器和事件较多的情况下,同步调用造成事件积压的情况.

下面是一些重要的属性：

eventQueue：数据结构为LinkedBlockingQueue,维护了队列前文所述的继承自SparkListenerEvent的样例类事件

通过spark.scheduler.listenerbus.eventqueue.capacity属性来设置阻塞队列的大小,默认为10000.
LinkedBlockingQueue如果不指定大小就为无限大,设置队列大小是为了可以抛出显式的异常而不是一个OOM异常.
private val eventQueue = new LinkedBlockingQueue[SparkListenerEvent](
	conf.get(LISTENER_BUS_EVENT_QUEUE_CAPACITY))
    
// org.apache.spark.internal.config
private[spark] val LISTENER_BUS_EVENT_QUEUE_CAPACITY =
	ConfigBuilder("spark.scheduler.listenerbus.eventqueue.capacity")
	.intConf
	.checkValue(_ > 0, "The capacity of listener bus event queue must be positive")
	.createWithDefault(10000)
eventCount：记录了未处理事件的个数,从eventQueue中弹出的事件并不代表被完全处理完,不能用队列大小代表未处理事件大小.

使用了AtomicLong原子量来保证修改的原子性
droppedEventsCounter：记录被丢弃事件的计数,eventQueue队列满了后,新产生的事件被丢弃.

使用了AtomicLong原子量来保证修改的原子性
started、stopped：记录队列的启动和停止状态

使用AtomicBoolean保证修改的原子性
dispatchThread：循环调用dispatch()方法的线程,tryOrStopSparkContext(sc)保证了遇到无法控制的异常时SparkContext能正常退出

下面是一些重要的方法

dispatch()：将队列中的事件循环取出并调用其特质ListenerBus实现的postToAll方法发送给对应的注册过的监听器,直到遇到了放在队列中的哨兵POISON_PILL就会停止发送事件

  private def dispatch(): Unit = LiveListenerBus.withinListenerThread.withValue(true) {
    var next: SparkListenerEvent = eventQueue.take()
    while (next != POISON_PILL) {
      val ctx = processingTime.time()
      try {
        super.postToAll(next)
      } finally {
        ctx.stop()
      }
      eventCount.decrementAndGet()
      next = eventQueue.take()
    }
    eventCount.decrementAndGet()
  }
post()：向队列中添加事件,如果队列满了,丢弃当前事件并记录日志.这是个生产者消费者模型,当队列满时生产者丢弃事件,但队列为空时消费者等待生产者.

  def post(event: SparkListenerEvent): Unit = {
    if (stopped.get()) {
      return
    }

    eventCount.incrementAndGet()
    if (eventQueue.offer(event)) {
      return
    }

    eventCount.decrementAndGet()
    droppedEvents.inc()
    droppedEventsCounter.incrementAndGet()
    if (logDroppedEvent.compareAndSet(false, true)) {
      // Only log the following message once to avoid duplicated annoying logs.
      logError(s"Dropping event from queue $name. " +
               "This likely means one of the listeners is too slow and cannot keep up with " +
               "the rate at which tasks are being started by the scheduler.")
    }
    logTrace(s"Dropping event $event")

    val droppedCount = droppedEventsCounter.get
    if (droppedCount > 0) {
      // Don't log too frequently
      if (System.currentTimeMillis() - lastReportTimestamp >= 60 * 1000) {
        // There may be multiple threads trying to decrease droppedEventsCounter.
        // Use "compareAndSet" to make sure only one thread can win.
        // And if another thread is increasing droppedEventsCounter, "compareAndSet" will fail and
        // then that thread will update it.
        if (droppedEventsCounter.compareAndSet(droppedCount, 0)) {
          val prevLastReportTimestamp = lastReportTimestamp
          lastReportTimestamp = System.currentTimeMillis()
          val previous = new java.util.Date(prevLastReportTimestamp)
          logWarning(s"Dropped $droppedCount events from $name since $previous.")
        }
      }
    }
  }
start()：启动dispatchThread,开始消费并分发事件

stop()：插入哨兵POISON_PILL,dispatchThread线程读取到哨兵时就会停止事件的分发

private[scheduler] def stop(): Unit = {
  if (!started.get()) {
    throw new IllegalStateException(s"Attempted to stop $name that has not yet started!")
  }
  if (stopped.compareAndSet(false, true)) {
    eventCount.incrementAndGet()
    eventQueue.put(POISON_PILL)
  }
  // this thread might be trying to stop itself as part of error handling -- we can't join
  // in that case.
  if (Thread.currentThread() != dispatchThread) {
    dispatchThread.join()
  }
}
LiveListenerBus
LiveListenerBus内部使用AsyncEventQueue作为核心,异步地发送SparkListenerEvents事件给已注册的SparkListeners监听器.

属性与AsyncEventQueue大同小异,下面介绍一些重要属性

queues：CopyOnWriteArrayList[AsyncEventQueue]保证了线程安全,维护一个AsyncEventQueue列表.
queuedEvents：mutable.ListBuffer[SparkListenerEvent],缓存再启动前接受到的事件.
下面介绍一下重要的方法,主要时启动停止和注册

addToQueue()方法将监听器注册到指定名称的队列,addToxxx()根据SparkListenerInterface未实现的回调函数进一步封装了addToQueue()方法

首先寻找队列是否存在,如果存在就注册,不存在就创建新队列并注册
private[spark] def addToQueue(
  listener: SparkListenerInterface,
  queue: String): Unit = synchronized {
  if (stopped.get()) {
    throw new IllegalStateException("LiveListenerBus is stopped.")
  }
  
  queues.asScala.find(_.name == queue) match {
    case Some(queue) =>
    queue.addListener(listener)
  
    case None =>
    val newQueue = new AsyncEventQueue(queue, conf, metrics, this)
    newQueue.addListener(listener)
    if (started.get()) {
      newQueue.start(sparkContext)
    }
    queues.add(newQueue)
  }
}
  
def addToEventLogQueue(listener: SparkListenerInterface): Unit = {
  addToQueue(listener, EVENT_LOG_QUEUE)
}
removeListener()：在queues中的所有队列中删除指定的监视器,如果删除后队列为空,就移除队列

post()：将事件发送到所有队列上.如果queuedEvents不为空,即存在缓存的事件,则将缓存的事件和当前事件一起发送.

def post(event: SparkListenerEvent): Unit = {
  if (stopped.get()) {
    return
  }
  
  metrics.numEventsPosted.inc()
  
  // If the event buffer is null, it means the bus has been started and we can avoid
  // synchronization and post events directly to the queues. This should be the most
  // common case during the life of the bus.
  if (queuedEvents == null) {
    postToQueues(event)
    return
  }
  
  // Otherwise, need to synchronize to check whether the bus is started, to make sure the thread
  // calling start() picks up the new event.
  synchronized {
    if (!started.get()) {
      queuedEvents += event
      return
    }
  }
  
  // If the bus was already started when the check above was made, just post directly to the
  // queues.
  postToQueues(event)
}
  
private def postToQueues(event: SparkListenerEvent): Unit = {
  val it = queues.iterator()
  while (it.hasNext()) {
    it.next().post(event)
  }
}
start()：启动每个队列,并发送queuedEvents中缓存的事件.每个队列就开始消费之前post的事件并调用postToAll()方法将事件发送给监视器.

stop()：停止所有队列并清空queue.


https://masterwangzx.com/2020/07/22/listener-bus/#listenerbus