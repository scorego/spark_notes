> Spark Driver用于提交用户应用程序，可以看做Spark的客户端。了解Spark Driver的初始化，有助于理解用户应用程序在客户端的处理过程。Spark Driver的初始化始终围绕着`SparkContext`的初始化。`SparkContext`可以视为Spark应用程序的发动机引擎，代表着和一个Spark群的连接，只有其初始化完毕，才能向Spark集群提交任务。



无论Spark提供的功能多么丰富，要想使用它，第一步就是初始化`SparkContext`。`SparkContext`的初始化过程实际也是对Driver的初始化，整个过程囊括了内部各个组件的初始化与准备，设计网络通信、分布式、消息、存储、计算、缓存、度量、清理、文件服务、Web UI的方方面面。

```scala
package org.apache.spark

/**
 * Main entry point for Spark functionality. A SparkContext represents the connection to a Spark
 * cluster, and can be used to create RDDs, accumulators and broadcast variables on that cluster.
 *
 * @note Only one `SparkContext` should be active per JVM. You must `stop()` the
 *   active `SparkContext` before creating a new one.
 * @param config a Spark Config object describing the application configuration. Any settings in
 *   this config overrides the default configs as well as system properties.
 */
class SparkContext(config: SparkConf) extends Logging {...}

/**
 * The SparkContext object contains a number of implicit conversions and parameters for use with
 * various Spark features.
 */
object SparkContext extends Logging {...}
```



# 一、 属性

`SparkContext`有以下重要属性：

伴生对象的属性：

```scala
private val activeContext: AtomicReference[SparkContext] = new AtomicReference[SparkContext](null)
```

实例属性：

```scala
// SparkConf
private var _conf: SparkConf = _

// 事件日志的路径。当spark.eventLog.enabled属性为true时启用，可以通过当spark.eventLog.dir指定，默认为/tmp/spark-events
private var _eventLogDir: Option[URI] = None
// 事件日志的压缩算法。当spark.eventLog.enabled和spark.eventLog.compress属性都为true时启用。可以通过spark.eventLog.compression.codec指定，支持lzf、snappy、lz4、ztsd四种
private var _eventLogCodec: Option[String] = None

// 事件总线。可以接受各个使用方的事件，并通过异步方式对事件进行匹配后调用SparkListener的不同方法。
private var _listenerBus: LiveListenerBus = _

// Spark的运行时环境
private var _env: SparkEnv = _

// 提供对Job、Stage等的监控信息。这是个低级的API，只能提供非常脆弱的一致性机制
private var _statusTracker: SparkStatusTracker = _

// 利用SparkStatusTracker的API，在控制台展示Stage的进度。由于SparkStatusTracker存在一致性问题，所以控制台的显示往往有一定的时延
private var _progressBar: Option[ConsoleProgressBar] = None

// SparkUI，Spark的用户界面。SparkUI将从各个SparkListener中读取数据并显示到Web界面
private var _ui: Option[SparkUI] = None

// Hadoop的配置信息。如果系统属性或环境变量中的SPARK_YARN_MODE位true，那么将会是YARN的配置，否则为Hadoop的配置
private var _hadoopConfiguration: Configuration = _

// executor的内存大小。可以通过spark.executor.memory属性指定
private var _executorMemory: Int = _

private var _schedulerBackend: SchedulerBackend = _

// 任务调度器，是调度系统的重要组件之一，按照调度算法对集群管理器已分配的资源进行二次调度后分配给任务。该调度依赖于DAGScheduler的调度。
private var _taskScheduler: TaskScheduler = _

// 心跳接收器。维护所有executor的心跳，还会将心跳信息交给TaskScheduler作进一步处理
private var _heartbeatReceiver: RpcEndpointRef = _

// DAG调度器，是调度系统的重要组件之一，负责创建Job、将DAG中的RDD划分到不同的Stage中、提交Stage等。SparkUI中有关Job和Stage的监控数据都来自此
@volatile private var _dagScheduler: DAGScheduler = _

// 当前应用标识。TaskScheduler启动后会创建应用标识，该属性通过TaskScheduler的applicationId方法获得
private var _applicationId: String = _
// 当前应用尝试执行的标识。Driver在执行时会多次尝试执行，每次都会生成一个标识
private var _applicationAttemptId: Option[String] = None

// 将事件持久化到存储的监听器。spark.eventLog.enabled为true时启用，为EventLoggingListener类
private var _eventLogger: Option[EventLoggingListener] = None

private var _driverLogger: Option[DriverLogger] = None

// executor动态分配管理器。可以根据工作负载动态调整executor的数量
private var _executorAllocationManager: Option[ExecutorAllocationManager] = None

// 上下文清理器。spark.cleaner.referenceTracking为true时启用，用异步方式清理那些超出应用作用域范围的RDD、ShuffleDependency、Broadcast等信息
private var _cleaner: Option[ContextCleaner] = None

// LiveListenerBus是否启动的标记
private var _listenerBusStarted: Boolean = false

// 用户设定的jar文件。可通过spark.jars指定
private var _jars: Seq[String] = _
// 用户设置的文件。可通过spark.files指定
private var _files: Seq[String] = _
// 可通过spark.archives指定
private var _archives: Seq[String] = _


private var _shutdownHookRef: AnyRef = _
private var _statusStore: AppStatusStore = _
private var _heartbeater: Heartbeater = _
private var _resources: immutable.Map[String, ResourceInformation] = _
private var _shuffleDriverComponents: ShuffleDriverComponents = _
private var _plugins: Option[PluginContainer] = None

// Manager of resource profiles
private var _resourceProfileManager: ResourceProfileManager = _


private[spark] val addedFiles = new ConcurrentHashMap[String, Long]().asScala
private[spark] val addedArchives = new ConcurrentHashMap[String, Long]().asScala
private[spark] val addedJars = new ConcurrentHashMap[String, Long]().asScala

// Keeps track of all persisted RDDs
private[spark] val persistentRdds = {
    val map: ConcurrentMap[Int, RDD[_]] = new MapMaker().weakValues().makeMap[Int, RDD[_]]()
    map.asScala
}

// Environment variables to pass to our executors.
private[spark] val executorEnvs = HashMap[String, String]()

// Set SPARK_USER for user who is running SparkContext.
val sparkUser = Utils.getCurrentUserName()
private[spark] var checkpointDir: Option[String] = None
```

# 二、`SparkContext.getOrCreate()`

`SparkContext`的伴生对象提供了`getOrCreate()`来获取`SparkContext`实例。

```scala
// 一个JVM只能有一个激活的SparkContext
def getOrCreate(config: SparkConf): SparkContext = {
    SPARK_CONTEXT_CONSTRUCTOR_LOCK.synchronized {
      if (activeContext.get() == null) {
        setActiveContext(new SparkContext(config))
      } else {
        if (config.getAll.nonEmpty) {
          logWarning("Using an existing SparkContext; some configuration may not take effect.")
        }
      }
      activeContext.get()
    }
}
```

本方法实际上就是使用`SparkConf`作为主构造器的参数来`new SparkContext(config)`。`SparkSession.builder().getOrCreate()`方法中会使用`SparkContext.getOrCreate(sparkConf)`来获取`SparkContext`。

# 三、构造器方法

```scala
主构造器：
class SparkContext(config: SparkConf) 

/**
  * Create a SparkContext that loads settings from system properties (for instance, when
  * launching with ./bin/spark-submit).
  */
def this() = this(new SparkConf())

def this(master: String, appName: String, conf: SparkConf) = this(SparkContext.updatedConf(conf, master, appName))

def this(
    master: String,
    appName: String,
    sparkHome: String = null,
    jars: Seq[String] = Nil,
    environment: Map[String, String] = Map()) = {
    this(SparkContext.updatedConf(new SparkConf(), master, appName, sparkHome, jars, environment))
}

private[spark] def this(master: String, appName: String) = this(master, appName, null, Nil, Map())

private[spark] def this(master: String, appName: String, sparkHome: String) = this(master, appName, sparkHome, Nil, Map())

private[spark] def this(master: String, appName: String, sparkHome: String, jars: Seq[String]) = this(master, appName, sparkHome, jars, Map())
```

# 四、初始化步骤

新建`SparkContext`实例的时候，主构造器逻辑如下：

```scala
private val creationSite: CallSite = Utils.getCallSite()

if (!config.get(EXECUTOR_ALLOW_SPARK_CONTEXT)) {	// "spark.executor.allowSparkContext"
    // 如果配置为false，则不允许excutor创建SparkContext
    SparkContext.assertOnDriver()
}

// 防止多个SparkContext同时处于active状态
SparkContext.markPartiallyConstructed(this)

val startTime = System.currentTimeMillis()

private[spark] val stopped: AtomicBoolean = new AtomicBoolean(false)

logInfo(s"Running Spark version $SPARK_VERSION")

private var _conf: SparkConf = _
private var _eventLogDir: Option[URI] = None
private var _eventLogCodec: Option[String] = None
private var _listenerBus: LiveListenerBus = _
private var _env: SparkEnv = _
private var _statusTracker: SparkStatusTracker = _
private var _progressBar: Option[ConsoleProgressBar] = None
private var _ui: Option[SparkUI] = None
private var _hadoopConfiguration: Configuration = _
private var _executorMemory: Int = _
private var _schedulerBackend: SchedulerBackend = _
private var _taskScheduler: TaskScheduler = _
private var _heartbeatReceiver: RpcEndpointRef = _
@volatile private var _dagScheduler: DAGScheduler = _
private var _applicationId: String = _
private var _applicationAttemptId: Option[String] = None
private var _eventLogger: Option[EventLoggingListener] = None
private var _driverLogger: Option[DriverLogger] = None
private var _executorAllocationManager: Option[ExecutorAllocationManager] = None
private var _cleaner: Option[ContextCleaner] = None
private var _listenerBusStarted: Boolean = false
private var _jars: Seq[String] = _
private var _files: Seq[String] = _
private var _archives: Seq[String] = _
private var _shutdownHookRef: AnyRef = _
private var _statusStore: AppStatusStore = _
private var _heartbeater: Heartbeater = _
private var _resources: immutable.Map[String, ResourceInformation] = _
private var _shuffleDriverComponents: ShuffleDriverComponents = _
private var _plugins: Option[PluginContainer] = None
private var _resourceProfileManager: ResourceProfileManager = _

private[spark] val addedFiles = new ConcurrentHashMap[String, Long]().asScala
private[spark] val addedArchives = new ConcurrentHashMap[String, Long]().asScala
private[spark] val addedJars = new ConcurrentHashMap[String, Long]().asScala

private[spark] val persistentRdds = {
    val map: ConcurrentMap[Int, RDD[_]] = new MapMaker().weakValues().makeMap[Int, RDD[_]]()
    map.asScala
}

// 传递给executor的环境变量
private[spark] val executorEnvs = HashMap[String, String]()

val sparkUser = Utils.getCurrentUserName()

private[spark] var checkpointDir: Option[String] = None

protected[spark] val localProperties = new InheritableThreadLocal[Properties] {
    override protected def childValue(parent: Properties): Properties = { Utils.cloneProperties(parent) }
    override protected def initialValue(): Properties = new Properties()
}

try {
    _conf = config.clone()
    _conf.validateSettings()
    _conf.set("spark.app.startTime", startTime.toString)

    if (!_conf.contains("spark.master")) { throw new SparkException("A master URL must be set in your configuration") }
    if (!_conf.contains("spark.app.name")) { throw new SparkException("An application name must be set in your configuration") }

    // 新建DriverLogger
    _driverLogger = DriverLogger(_conf)

    val resourcesFileOpt = conf.get(DRIVER_RESOURCES_FILE)	// "spark.driver.resourcesFile"
    _resources = getOrDiscoverAllResources(_conf, SPARK_DRIVER_PREFIX, resourcesFileOpt)
    logResourceInfo(SPARK_DRIVER_PREFIX, _resources)

    logInfo(s"Submitted application: $appName")

    // System property spark.yarn.app.id must be set if user code ran by AM on a YARN cluster
    if (master == "yarn" && deployMode == "cluster" && !_conf.contains("spark.yarn.app.id")) {
        throw new SparkException("Detected yarn cluster mode, but isn't running on a cluster. " +
                                 "Deployment to YARN is not supported directly by SparkContext. Please use spark-submit.")
    }

    if (_conf.getBoolean("spark.logConf", false)) { logInfo("Spark configuration:\n" + _conf.toDebugString) }

    // Set Spark driver host and port system properties. This explicitly sets the configuration
    // instead of relying on the default value of the config constant.
    _conf.set(DRIVER_HOST_ADDRESS, _conf.get(DRIVER_HOST_ADDRESS))
    _conf.setIfMissing(DRIVER_PORT, 0)

    _conf.set(EXECUTOR_ID, SparkContext.DRIVER_IDENTIFIER)	// 将"spark.executor.id"设为"driver"

    _jars = Utils.getUserJars(_conf)  // "spark.jars"
    _files = _conf.getOption(FILES.key).map(_.split(",")).map(_.filter(_.nonEmpty)).toSeq.flatten  // "spark.files"
    _archives = _conf.getOption(ARCHIVES.key).map(Utils.stringToSeq).toSeq.flatten	 // "spark.archives"

    _eventLogDir = if (isEventLogEnabled) {	// "spark.eventLog.enabled"
            val unresolvedDir = conf.get(EVENT_LOG_DIR).stripSuffix("/")	// "spark.eventLog.dir"
            Some(Utils.resolveURI(unresolvedDir))
        } else {
            None
        }

    _eventLogCodec = {
        val compress = _conf.get(EVENT_LOG_COMPRESS)	// "spark.eventLog.compress"
        if (compress && isEventLogEnabled) {		// "spark.eventLog.enabled"
            // "spark.eventLog.compression.codec"
            Some(_conf.get(EVENT_LOG_COMPRESSION_CODEC)).map(CompressionCodec.getShortName) 
        } else {
            None
        }
    }

    // 新建事件总线
    _listenerBus = new LiveListenerBus(_conf)
    _resourceProfileManager = new ResourceProfileManager(_conf, _listenerBus)

    // Initialize the app status store and listener before SparkEnv is created so that it gets all events.
    val appStatusSource = AppStatusSource.createSource(conf)
    _statusStore = AppStatusStore.createLiveStore(conf, appStatusSource)
    listenerBus.addToStatusQueue(_statusStore.listener.get)

    // 新建SparkEnv(cache, map output tracker, etc)
    _env = createSparkEnv(_conf, isLocal, listenerBus)
    SparkEnv.set(_env)

    // If running the REPL, register the repl's output dir with the file server.
    _conf.getOption("spark.repl.class.outputDir").foreach { path =>
        val replUri = _env.rpcEnv.fileServer.addDirectory("/classes", new File(path))
        _conf.set("spark.repl.class.uri", replUri)
    }

    _statusTracker = new SparkStatusTracker(this, _statusStore)

    _progressBar = if (_conf.get(UI_SHOW_CONSOLE_PROGRESS)) {
            Some(new ConsoleProgressBar(this))
        } else {
            None
        }

    _ui = if (conf.get(UI_ENABLED)) {
            Some(SparkUI.create(Some(this), _statusStore, _conf, _env.securityManager, appName, "", startTime))
        } else {
            // For tests, do not enable the UI
            None
        }
    
    // Bind the UI before starting the task scheduler to communicate the bound port to the cluster manager properly
    _ui.foreach(_.bind())

    _hadoopConfiguration = SparkHadoopUtil.get.newConfiguration(_conf)
    // Performance optimization: this dummy call to .size() triggers eager evaluation of
    // Configuration's internal  `properties` field, guaranteeing that it will be computed and
    // cached before SessionState.newHadoopConf() uses `sc.hadoopConfiguration` to create
    // a new per-session Configuration. If `properties` has not been computed by that time
    // then each newly-created Configuration will perform its own expensive IO and XML
    // parsing to load configuration defaults and populate its own properties. By ensuring
    // that we've pre-computed the parent's properties, the child Configuration will simply
    // clone the parent's properties.
    _hadoopConfiguration.size()

    // Add each JAR given through the constructor
    if (jars != null) {
        jars.foreach(jar => addJar(jar, true))
        if (addedJars.nonEmpty) {
            _conf.set("spark.app.initial.jar.urls", addedJars.keys.toSeq.mkString(","))
        }
    }

    if (files != null) {
        files.foreach(file => addFile(file, false, true))
        if (addedFiles.nonEmpty) {
            _conf.set("spark.app.initial.file.urls", addedFiles.keys.toSeq.mkString(","))
        }
    }

    if (archives != null) {
        archives.foreach(file => addFile(file, false, true, isArchive = true))
        if (addedArchives.nonEmpty) {
            _conf.set("spark.app.initial.archive.urls", addedArchives.keys.toSeq.mkString(","))
        }
    }

    _executorMemory = _conf.getOption(EXECUTOR_MEMORY.key)			// "spark.executor.memory"
        .orElse(Option(System.getenv("SPARK_EXECUTOR_MEMORY")))		// 
        .orElse(Option(System.getenv("SPARK_MEM"))
        .map(warnSparkMem))
        .map(Utils.memoryStringToMb)
        .getOrElse(1024)

    // Convert java options to env vars as a work around since we can't set env vars directly in sbt.
    for { (envKey, propKey) <- Seq(("SPARK_TESTING", IS_TESTING.key))
         value <- Option(System.getenv(envKey)).orElse(Option(System.getProperty(propKey)))} { executorEnvs(envKey) = value }
    Option(System.getenv("SPARK_PREPEND_CLASSES")).foreach { v => executorEnvs("SPARK_PREPEND_CLASSES") = v }
    executorEnvs("SPARK_EXECUTOR_MEMORY") = executorMemory + "m"
    executorEnvs ++= _conf.getExecutorEnv
    executorEnvs("SPARK_USER") = sparkUser

    _shuffleDriverComponents = ShuffleDataIOUtils.loadShuffleDataIO(config).driver()
    _shuffleDriverComponents.initializeApplication().asScala.foreach { case (k, v) =>
        _conf.set(ShuffleDataIOUtils.SHUFFLE_SPARK_CONF_PREFIX + k, v)
    }

    // We need to register "HeartbeatReceiver" before "createTaskScheduler" because Executor will
    // retrieve "HeartbeatReceiver" in the constructor. (SPARK-6640)
    _heartbeatReceiver = env.rpcEnv.setupEndpoint(HeartbeatReceiver.ENDPOINT_NAME, new HeartbeatReceiver(this))

    // Initialize any plugins before the task scheduler is initialized.
    _plugins = PluginContainer(this, _resources.asJava)

    // Create and start the scheduler
    val (sched, ts) = SparkContext.createTaskScheduler(this, master, deployMode)
    _schedulerBackend = sched
    _taskScheduler = ts
    _dagScheduler = new DAGScheduler(this)
    _heartbeatReceiver.ask[Boolean](TaskSchedulerIsSet)

    val _executorMetricsSource = if (_conf.get(METRICS_EXECUTORMETRICS_SOURCE_ENABLED)) {
            Some(new ExecutorMetricsSource)
        } else {
            None
        }

    // create and start the heartbeater for collecting memory metrics
    _heartbeater = new Heartbeater(
        () => SparkContext.this.reportHeartBeat(_executorMetricsSource),
        "driver-heartbeater",
        conf.get(EXECUTOR_HEARTBEAT_INTERVAL))
    _heartbeater.start()

    // start TaskScheduler after taskScheduler sets DAGScheduler reference in DAGScheduler's constructor
    _taskScheduler.start()

    _applicationId = _taskScheduler.applicationId()
    _applicationAttemptId = _taskScheduler.applicationAttemptId()
    _conf.set("spark.app.id", _applicationId)
    if (_conf.get(UI_REVERSE_PROXY)) {		// "spark.ui.reverseProxy"
        // UI_REVERSE_PROXY_URL: "spark.ui.reverseProxyUrl"
        val proxyUrl = _conf.get(UI_REVERSE_PROXY_URL.key, "").stripSuffix("/") + "/proxy/" + _applicationId	
        System.setProperty("spark.ui.proxyBase", proxyUrl)
    }
    _ui.foreach(_.setAppId(_applicationId))
    _env.blockManager.initialize(_applicationId)
    FallbackStorage.registerBlockManagerIfNeeded(_env.blockManager.master, _conf)

    // The metrics system for Driver need to be set spark.app.id to app ID.
    // So it should start after we get app ID from the task scheduler and set spark.app.id.
    _env.metricsSystem.start(_conf.get(METRICS_STATIC_SOURCES_ENABLED))		// "spark.metrics.staticSources.enabled"
    // Attach the driver metrics servlet handler to the web ui after the metrics system is started.
    _env.metricsSystem.getServletHandlers.foreach(handler => ui.foreach(_.attachHandler(handler)))

    _eventLogger = if (isEventLogEnabled) {
            val logger = new EventLoggingListener(_applicationId, 
                                                  _applicationAttemptId, 
                                                  _eventLogDir.get, 
                                                  _conf, 
                                                  _hadoopConfiguration)
            logger.start()
            listenerBus.addToEventLogQueue(logger)
            Some(logger)
        } else {
            None
        }

    _cleaner = if (_conf.get(CLEANER_REFERENCE_TRACKING)) {	// "spark.cleaner.referenceTracking"
            Some(new ContextCleaner(this, _shuffleDriverComponents))
        } else {
            None
        }
    _cleaner.foreach(_.start())

    val dynamicAllocationEnabled = Utils.isDynamicAllocationEnabled(_conf)
    _executorAllocationManager = if (dynamicAllocationEnabled) {
            schedulerBackend match {
                case b: ExecutorAllocationClient =>
                    Some(new ExecutorAllocationManager(schedulerBackend.asInstanceOf[ExecutorAllocationClient],
                                                       listenerBus, 
                                                       _conf,
                        							   cleaner = cleaner, 
                                                       resourceProfileManager = resourceProfileManager))
                case _ => None
            }
        } else {
            None
        }
    _executorAllocationManager.foreach(_.start())

    setupAndStartListenerBus()
    postEnvironmentUpdate()
    postApplicationStart()

    // Post init
    _taskScheduler.postStartHook()
    if (isLocal) { _env.metricsSystem.registerSource(Executor.executorSourceLocalModeOnly) }
    _env.metricsSystem.registerSource(_dagScheduler.metricsSource)
    _env.metricsSystem.registerSource(new BlockManagerSource(_env.blockManager))
    _env.metricsSystem.registerSource(new JVMCPUSource())
    _executorMetricsSource.foreach(_.register(_env.metricsSystem))
    _executorAllocationManager.foreach { e => _env.metricsSystem.registerSource(e.executorAllocationManagerSource) }
    appStatusSource.foreach(_env.metricsSystem.registerSource(_))
    _plugins.foreach(_.registerMetrics(applicationId))
    // Make sure the context is stopped if the user forgets about it. This avoids leaving
    // unfinished event logs around after the JVM exits cleanly. It doesn't help if the JVM is killed, though.
    logDebug("Adding shutdown hook") // force eager creation of logger
    _shutdownHookRef = ShutdownHookManager.addShutdownHook(ShutdownHookManager.SPARK_CONTEXT_SHUTDOWN_PRIORITY) { () => 
            logInfo("Invoking stop() from shutdown hook")
            try {
                stop()
            } catch {
                case e: Throwable => logWarning("Ignoring Exception while stopping SparkContext from shutdown hook", e)
            }
    }
} catch {
    case NonFatal(e) => logError("Error initializing SparkContext.", e)
    try {
        stop()
    } catch {
        case NonFatal(inner) => logError("Error stopping SparkContext after init error.", inner)
    } finally {
        throw e
    }
}
```

## 1. 创建`SparkEnv`

`SparkEnv`为executor提供运行环境，driver中也会包含`SparkEnv`吗，这是为了保证local模式下任务的执行。`SparkContext`中初始`SparkEnv`的代码如下：

```scala
def isLocal: Boolean = Utils.isLocalMaster(_conf)
_listenerBus = new LiveListenerBus(_conf)
_env = createSparkEnv(_conf, isLocal, listenerBus)
SparkEnv.set(_env)

======================================================
// This function allows components created by SparkEnv to be mocked in unit tests:
private[spark] def createSparkEnv( conf: SparkConf, isLocal: Boolean, listenerBus: LiveListenerBus): SparkEnv = {
    SparkEnv.createDriverEnv(conf, isLocal, listenerBus, SparkContext.numDriverCores(master, conf))
}
```

因为`SparkEnv`内的很多组件都将向`LiveListenerBus`的事件队列中投递事件，所以会首先创建`LiveListenerBus`，并用来创建`SparkEnv`。创建`SparkEnv`对象是通过`SparkEnv.createDriverEnv()`方法创建的，之后会调用`SparkEnv`伴生对象的`set()`方法将创建的`SparkEnv`实例设置到伴生对象的`env`属性中，这便于在任何需要`SparkEnv`的地方通过`get()`方法获取`SparkEnv`对象。`SparkEnv.createDriverEnv()`代码如下：

```scala
/**
   * Create a SparkEnv for the driver.
   */
private[spark] def createDriverEnv(
    conf: SparkConf,
    isLocal: Boolean,
    listenerBus: LiveListenerBus,
    numCores: Int,
    mockOutputCommitCoordinator: Option[OutputCommitCoordinator] = None): SparkEnv = {
    
    // spark.driver.host  driver实例的host
    assert(conf.contains(DRIVER_HOST_ADDRESS), s"${DRIVER_HOST_ADDRESS.key} is not set on the driver!")
    
    // spark.driver.port
    assert(conf.contains(DRIVER_PORT), s"${DRIVER_PORT.key} is not set on the driver!")
    
    // "spark.driver.bindAddress".fallbackConf("spark.driver.host")
    val bindAddress = conf.get(DRIVER_BIND_ADDRESS)
    // "spark.driver.host"
    val advertiseAddress = conf.get(DRIVER_HOST_ADDRESS)
    // "spark.driver.port"
    val port = conf.get(DRIVER_PORT)
    
    // IO加密的秘钥。当"spark.io.encryption.enabled"为true是，会创建秘钥
    val ioEncryptionKey = if (conf.get(IO_ENCRYPTION_ENABLED)) {
        Some(CryptoStreamUtils.createKey(conf))
    } else {
        None
    }
    
    create(
        conf,
        // "driver"
        SparkContext.DRIVER_IDENTIFIER,
        bindAddress,
        advertiseAddress,
        Option(port),
        isLocal,
        numCores,
        ioEncryptionKey,
        listenerBus = listenerBus,
        mockOutputCommitCoordinator = mockOutputCommitCoordinator
    )
}
```

## 2. 创建`SparkUI`

Spark提供了Web页面来浏览监控数据，而且Master、Worker、Driver根据自身功能提供了不同内容的Web监控页面，它们都使用了统一的Web框架WebUI。Master、Worker、Driver分别使用MasterWebUI、WorkerWebUI、SparkUI，他们都继承自WebUI。此外，YARN或Mesos模式下还有WebUI的另一个扩展实现HistoryServer。

```scala
_ui =
	// "spark.ui.enabled"
    if (conf.get(UI_ENABLED)) {
        Some(SparkUI.create(Some(this), _statusStore, _conf, _env.securityManager, appName, "", startTime))
    } else {
        // For tests, do not enable the UI
        None
    }

// Bind the UI before starting the task scheduler to communicate the bound port to the cluster manager properly
_ui.foreach(_.bind())

_ui.foreach(_.setAppId(_applicationId))
```

## 3. 创建心跳接收器

Driver需要使用心跳接收器来掌控executor的状态。`SparkContext`中创建`HeartbeatReceiver`的代码如下：

```scala
// We need to register "HeartbeatReceiver" before "createTaskScheduler" because Executor will retrieve "HeartbeatReceiver" in the constructor. (SPARK-6640)
_heartbeatReceiver = env.rpcEnv.setupEndpoint(HeartbeatReceiver.ENDPOINT_NAME, new HeartbeatReceiver(this))
```

`SparkEnv`的子组件`RpcEnv`的`setupEndpoint()`方法的作用是注册`RpcEndpoint`。

## 4. 创建和启动调度系统

`TaskScheduler`是`SparkContext`的重要组成部分，负责请求集群管理器给应用程序分配并运行executor（一级调度）、给任务分配executor并运行任务（二级调度）。`TaskScheduler`可以看做任务调度的客户端，`DAGScheduler`主要用于在任务正式交给`TaskSchedulerImpl`提交之前做一些准备工作，包括创建Job、将DAG中的RDD划分到不同的Stage、提交Stage等。

```scala
// Create and start the scheduler
val (sched, ts) = SparkContext.createTaskScheduler(this, master, deployMode)
_schedulerBackend = sched
_taskScheduler = ts
_dagScheduler = new DAGScheduler(this)
// HeartbeatReceiver接收到TaskSchedulerIsSet会将SparkContext的_taskScheduler属性设置到自己的scheduler属性中去
_heartbeatReceiver.ask[Boolean](TaskSchedulerIsSet)

// start TaskScheduler after taskScheduler sets DAGScheduler reference in DAGScheduler's constructor
_taskScheduler.start()
```

`SparkContext.createTaskScheduler()`方法代码如下：

```scala
/**
  * Create a task scheduler based on a given master URL.
  * Return a 2-tuple of the scheduler backend and the task scheduler.
  */
private def createTaskScheduler(
    sc: SparkContext,
    master: String,
    deployMode: String): (SchedulerBackend, TaskScheduler) = {
    import SparkMasterRegex._

    // When running locally, don't try to re-execute tasks on failure.
    val MAX_LOCAL_TASK_FAILURES = 1

    // Ensure that default executor's resources satisfies one or more tasks requirement.
    // This function is for cluster managers that don't set the executor cores config, for
    // others its checked in ResourceProfile.
    def checkResourcesPerTask(executorCores: Int): Unit = {
        val taskCores = sc.conf.get(CPUS_PER_TASK)
        if (!sc.conf.get(SKIP_VALIDATE_CORES_TESTING)) {
            validateTaskCpusLargeEnough(sc.conf, executorCores, taskCores)
        }
        val defaultProf = sc.resourceProfileManager.defaultResourceProfile
        ResourceUtils.warnOnWastedResources(defaultProf, sc.conf, Some(executorCores))
    }

    master match {
      	case "local" =>
            checkResourcesPerTask(1)
            val scheduler = new TaskSchedulerImpl(sc, MAX_LOCAL_TASK_FAILURES, isLocal = true)
            val backend = new LocalSchedulerBackend(sc.getConf, scheduler, 1)
            scheduler.initialize(backend)
            (backend, scheduler)

        case LOCAL_N_REGEX(threads) =>
            def localCpuCount: Int = Runtime.getRuntime.availableProcessors()
            // local[*] estimates the number of cores on the machine; local[N] uses exactly N threads.
            val threadCount = if (threads == "*") localCpuCount else threads.toInt
            if (threadCount <= 0) {
                throw new SparkException(s"Asked to run locally with $threadCount threads")
            }
            checkResourcesPerTask(threadCount)
            val scheduler = new TaskSchedulerImpl(sc, MAX_LOCAL_TASK_FAILURES, isLocal = true)
            val backend = new LocalSchedulerBackend(sc.getConf, scheduler, threadCount)
            scheduler.initialize(backend)
            (backend, scheduler)

        case LOCAL_N_FAILURES_REGEX(threads, maxFailures) =>
            def localCpuCount: Int = Runtime.getRuntime.availableProcessors()
            // local[*, M] means the number of cores on the computer with M failures
            // local[N, M] means exactly N threads with M failures
            val threadCount = if (threads == "*") localCpuCount else threads.toInt
            checkResourcesPerTask(threadCount)
            val scheduler = new TaskSchedulerImpl(sc, maxFailures.toInt, isLocal = true)
            val backend = new LocalSchedulerBackend(sc.getConf, scheduler, threadCount)
            scheduler.initialize(backend)
            (backend, scheduler)
		
        // """spark://(.*)""".r
        case SPARK_REGEX(sparkUrl) =>
            val scheduler = new TaskSchedulerImpl(sc)
            val masterUrls = sparkUrl.split(",").map("spark://" + _)
            val backend = new StandaloneSchedulerBackend(scheduler, sc, masterUrls)
            scheduler.initialize(backend)
            (backend, scheduler)

        case LOCAL_CLUSTER_REGEX(numWorkers, coresPerWorker, memoryPerWorker) =>
            checkResourcesPerTask(coresPerWorker.toInt)
            // Check to make sure memory requested <= memoryPerWorker. Otherwise Spark will just hang.
            val memoryPerWorkerInt = memoryPerWorker.toInt
            if (sc.executorMemory > memoryPerWorkerInt) {
                throw new SparkException(
                    "Asked to launch cluster with %d MiB RAM / worker but requested %d MiB/worker".format(
                        memoryPerWorkerInt, sc.executorMemory))
            }

            // For host local mode setting the default of SHUFFLE_HOST_LOCAL_DISK_READING_ENABLED
            // to false because this mode is intended to be used for testing and in this case all the
            // executors are running on the same host. So if host local reading was enabled here then
            // testing of the remote fetching would be secondary as setting this config explicitly to
            // false would be required in most of the unit test (despite the fact that remote fetching
            // is much more frequent in production).
            sc.conf.setIfMissing(SHUFFLE_HOST_LOCAL_DISK_READING_ENABLED, false)

            val scheduler = new TaskSchedulerImpl(sc)
            val localCluster = new LocalSparkCluster(
                numWorkers.toInt, coresPerWorker.toInt, memoryPerWorkerInt, sc.conf)
            val masterUrls = localCluster.start()
            val backend = new StandaloneSchedulerBackend(scheduler, sc, masterUrls)
            scheduler.initialize(backend)
            backend.shutdownCallback = (backend: StandaloneSchedulerBackend) => {
                localCluster.stop()
            }
            (backend, scheduler)

        case masterUrl =>
            val cm = getClusterManager(masterUrl) match {
                case Some(clusterMgr) => clusterMgr
                case None => throw new SparkException("Could not parse Master URL: '" + master + "'")
            }
            try {
                val scheduler = cm.createTaskScheduler(sc, masterUrl)
                val backend = cm.createSchedulerBackend(sc, masterUrl, scheduler)
                cm.initialize(scheduler, backend)
                (backend, scheduler)
            } catch {
                case se: SparkException => throw se
                case NonFatal(e) =>
                throw new SparkException("External scheduler cannot be instantiated", e)
            }
    }
}
```

根据不同的master地址，创建任务调度器的方式也不同。

## 5. 初始化块管理器BlockManager

`BlockManager`是`SparkEnv`的组件之一，囊括了存储体系的所有组件和功能，是存储体系中最重要的组件。

```scala
_env.blockManager.initialize(_applicationId)
```

## 6. 启动度量系统

`MetricsSystem`是`SparkEnv`的内部组件之一，是整个Spark应用程序的度量系统。其对Source和Sink进行封装，将Source的数据输出到不同的Sink。

```scala
// The metrics system for Driver need to be set spark.app.id to app ID.
// So it should start after we get app ID from the task scheduler and set spark.app.id.
_env.metricsSystem.start(_conf.get(METRICS_STATIC_SOURCES_ENABLED))
// Attach the driver metrics servlet handler to the web ui after the metrics system is started.
_env.metricsSystem.getServletHandlers.foreach(handler => ui.foreach(_.attachHandler(handler)))
```

## 7. 创建事件日志监听器

```scala
 _eventLogger =
      if (isEventLogEnabled) {
        val logger =
          new EventLoggingListener(_applicationId, _applicationAttemptId, _eventLogDir.get,
            _conf, _hadoopConfiguration)
        logger.start()
        listenerBus.addToEventLogQueue(logger)
        Some(logger)
      } else {
        None
      }
```

`EventLoggingListener`是将事件持久化到存储的监听器，是`SparkContext`中的可选组件。其最核心的方法是`logEvent()`，用于将事件转换为Json字符串后写入日志文件，实现如下：

```scala
/** Log the event as JSON. */
private def logEvent(event: SparkListenerEvent, flushLogger: Boolean = false): Unit = {
    val eventJson = JsonProtocol.sparkEventToJson(event)
    logWriter.writeEvent(compact(render(eventJson)), flushLogger)
    if (testing) {
        loggedEvents += eventJson
    }
}
```

## 8. 创建和启动`ExecutorAllocationManager`

`ExecutorAllocationManager`是基于工作负载动态分配和删除executor的代理。

```scala
val dynamicAllocationEnabled = Utils.isDynamicAllocationEnabled(_conf)
_executorAllocationManager =
      if (dynamicAllocationEnabled) {
        schedulerBackend match {
          case b: ExecutorAllocationClient =>
            Some(new ExecutorAllocationManager(
              	schedulerBackend.asInstanceOf[ExecutorAllocationClient], 
                listenerBus, 
                _conf,
              	cleaner = cleaner, 
                resourceProfileManager = resourceProfileManager))
          case _ =>
            None
        }
      } else {
        None
      }
_executorAllocationManager.foreach(_.start())

_executorAllocationManager.foreach { e => _env.metricsSystem.registerSource(e.executorAllocationManagerSource) }
```

可见，当`SchedulerBackend`的实现类同时实现了`ExecutorAllocationClient`特质的情况下，会创建`ExecutorAllocationManager`。只有`CoarseGrainedSchedulerBackend`和其子类实现了这个特质。

## 9. 创建和启动`ContextCleaner`

`ContextCleaner`用于清理那些超出应用范围的RDD、Shuffle对应的map任务状态、Shuffle元数据、Broadcast对象及RDD的Checkpoint数据。

```scala
_cleaner =
	  // "spark.cleaner.referenceTracking"
      if (_conf.get(CLEANER_REFERENCE_TRACKING)) {
        Some(new ContextCleaner(this, _shuffleDriverComponents))
      } else {
        None
      }
_cleaner.foreach(_.start())
```

`start()`方法如下：

```scala
/** Start the cleaner. */
def start(): Unit = {
    cleaningThread.setDaemon(true)
    cleaningThread.setName("Spark Context Cleaner")
    cleaningThread.start()
    periodicGCService.scheduleAtFixedRate(
        () => System.gc(),
        periodicGCInterval, // sc.conf.get(CLEANER_PERIODIC_GC_INTERVAL)   "spark.cleaner.periodicGC.interval"  
        periodicGCInterval, // sc.conf.get(CLEANER_PERIODIC_GC_INTERVAL)   "spark.cleaner.periodicGC.interval"
        TimeUnit.SECONDS)
}
```

## 10. 额外的`SparkListener`与启动事件总线

```scala
setupAndStartListenerBus()

============================================
/**
  * Registers listeners specified in spark.extraListeners, then starts the listener bus.
  * This should be called after all internal listeners have been registered with the listener bus
  * (e.g. after the web UI and event logging listeners have been registered).
  */
private def setupAndStartListenerBus(): Unit = {
    try {
        // 获取用户自定义的SparkListener类名，配置key是"spark.extraListeners"
        conf.get(EXTRA_LISTENERS).foreach { classNames =>
            // 通过反射生成每一个自定义的SparkListener类的实例，并添加到事件总线的监听器列表中
            val listeners = Utils.loadExtensions(classOf[SparkListenerInterface], classNames, conf)
            listeners.foreach { listener =>
                listenerBus.addToSharedQueue(listener)
                logInfo(s"Registered listener ${listener.getClass().getName()}")
            }
        }
    } catch {
        case e: Exception =>
        try {
            stop()
        } finally {
            throw new SparkException(s"Exception when registering SparkListener", e)
        }
    }

    listenerBus.start(this, _env.metricsSystem)
    _listenerBusStarted = true
}
```

## 11. 读取用户指定的文件



```scala
// "spark.jars"
_jars = Utils.getUserJars(_conf)
"spark.files"
_files = _conf.getOption(FILES.key).map(_.split(",")).map(_.filter(_.nonEmpty)).toSeq.flatten
// "spark.archives"
_archives = _conf.getOption(ARCHIVES.key).map(Utils.stringToSeq).toSeq.flatten

// Add each JAR given through the constructor
if (jars != null) {
    jars.foreach(jar => addJar(jar, true))
    if (addedJars.nonEmpty) {
        _conf.set("spark.app.initial.jar.urls", addedJars.keys.toSeq.mkString(","))
    }
}

if (files != null) {
    files.foreach(file => addFile(file, false, true))
    if (addedFiles.nonEmpty) {
        _conf.set("spark.app.initial.file.urls", addedFiles.keys.toSeq.mkString(","))
    }
}

if (archives != null) {
    archives.foreach(file => addFile(file, false, true, isArchive = true))
    if (addedArchives.nonEmpty) {
        _conf.set("spark.app.initial.archive.urls", addedArchives.keys.toSeq.mkString(","))
    }
}
```

`addJar()`方法将调用`env.rpcEnv.fileServer.addJar(file)`将Jar文件添加到Driver的RPC环境中。使用`addFile()`方法添加的文件将会被此Spark job相关的所有节点下载，调用的是`env.rpcEnv.fileServer.addFile(new File(uri.getPath))`。

通过`addJar()`/`addFile()`可以将各种任务执行所依赖的文件添加到Driver的RPC环境中，这样各个executor节点就可以使用RPC从Driver将文件下载到本地，以供任务执行。

`addJar()`/`addFile()`方法的最后都调用了`postEnviormentUpdate()`方法更新环境。

## 12. 一些其他工作

```scala
// 向事件总线投递SparkListenerApplicationStart事件
postApplicationStart()

// 等待SchedulerBackend准备完成
_taskScheduler.postStartHook()

// 向度量系统注册Source
if (isLocal) {
    _env.metricsSystem.registerSource(Executor.executorSourceLocalModeOnly)
}
_env.metricsSystem.registerSource(_dagScheduler.metricsSource)
_env.metricsSystem.registerSource(new BlockManagerSource(_env.blockManager))
_env.metricsSystem.registerSource(new JVMCPUSource())
_executorMetricsSource.foreach(_.register(_env.metricsSystem))
_executorAllocationManager.foreach { e => _env.metricsSystem.registerSource(e.executorAllocationManagerSource)}
appStatusSource.foreach(_env.metricsSystem.registerSource(_))
_plugins.foreach(_.registerMetrics(applicationId))

// 添加SparkContext的关闭钩子，使得JVM退出之前调用SparkContext的stop()方法
_shutdownHookRef = ShutdownHookManager.addShutdownHook(
    ShutdownHookManager.SPARK_CONTEXT_SHUTDOWN_PRIORITY) { () =>
    logInfo("Invoking stop() from shutdown hook")
    try {
        stop()
    } catch {
        case e: Throwable =>
        logWarning("Ignoring Exception while stopping SparkContext from shutdown hook", e)
    }
}

// In order to prevent multiple SparkContexts from being active at the same time, mark this context as having finished construction.
// NOTE: this must be placed at the end of the SparkContext constructor.
SparkContext.setActiveContext(this)
```

