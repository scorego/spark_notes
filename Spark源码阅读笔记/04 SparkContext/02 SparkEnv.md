> Spark对任务的计算都依托于executor，所有的executor都有自己的Spark执行环境`SparkEnv`。`SparkEnv`提供了多种多样内部组件来实现不同的功能。在local模式下Driver会创建executor，local-cluster部署模式或者Standalone部署模式下Worker另起的`CoarseGrainedExecutorBackend`进程中也会创建executor，所以`SparkEnv`存在于Driver或者`CoarseGrainedExecutorBackend`进程中。



```scala
package org.apache.spark

/**
 * :: DeveloperApi ::
 * Holds all the runtime environment objects for a running Spark instance (either master or worker),
 * including the serializer, RpcEnv, block manager, map output tracker, etc. Currently
 * Spark code finds the SparkEnv through a global variable, so all the threads can access the same
 * SparkEnv. It can be accessed by SparkEnv.get (e.g. after creating a SparkContext).
 */
@DeveloperApi
class SparkEnv (
    val executorId: String,
    private[spark] val rpcEnv: RpcEnv,
    val serializer: Serializer,
    val closureSerializer: Serializer,
    val serializerManager: SerializerManager,
    val mapOutputTracker: MapOutputTracker,
    val shuffleManager: ShuffleManager,
    val broadcastManager: BroadcastManager,
    val blockManager: BlockManager,
    val securityManager: SecurityManager,
    val metricsSystem: MetricsSystem,
    val memoryManager: MemoryManager,
    val outputCommitCoordinator: OutputCommitCoordinator,
    val conf: SparkConf) extends Logging {...}

object SparkEnv extends Logging {...}
```

# 一、 内部组件

创建`SparkEnv`主要使用`SparkEnv`的`createDriverEnv()`和`createExecutorEnv()`方法，最终这些方法都会调用`create()`方法。

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
    
    assert(conf.contains(DRIVER_HOST_ADDRESS), s"${DRIVER_HOST_ADDRESS.key} is not set on the driver!")
    assert(conf.contains(DRIVER_PORT), s"${DRIVER_PORT.key} is not set on the driver!")
    
    // "spark.driver.bindAddress"
    val bindAddress = conf.get(DRIVER_BIND_ADDRESS)
    // "spark.driver.host"
    val advertiseAddress = conf.get(DRIVER_HOST_ADDRESS)
    // "spark.driver.port"
    val port = conf.get(DRIVER_PORT)
    
    val ioEncryptionKey = if (conf.get(IO_ENCRYPTION_ENABLED)) {
        Some(CryptoStreamUtils.createKey(conf))
    } else {
        None
    }
    
    create(
        conf,
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

`SparkEnv`的内部组件有很多：

| 名称                      | 说明                                                         |
| ------------------------- | ------------------------------------------------------------ |
| SecurityManager           | 主要对账户、权限及身份认证进行设置与管理。                   |
| RpcEnv                    | 各个组件之间通信的执行环境。                                 |
| SerializerManager         | Spark 中很多对象在通用网络传输或者写入存储体系时，都需要序列化。 |
| BroadcastManager          | 用于将配置信息和序列化后的RDD、Job以及ShuffleDependency等信息在本地存储。 |
| MapOutputTracker          | 用于跟踪Map阶段任务的输出状态，此状态便于Reduce阶段任务获取地址及中间结果。 |
| ShuffleManager            | 负责管理本地及远程的Block数据的shuffle操作。                 |
| MemoryManager             | 一个抽象的内存管理器，用于执行内存如何在执行和存储之间共享。 |
| NettyBlockTransferService | 使用Netty提供的异步事件驱动的网络应用框架，提供Web服务及客户端，获取远程节点上Block的集合。 |
| BlockManagerMaster        | 负责对BlockManager的管理和协调。                             |
| BlockManager              | 负责对Block的管理，管理整个Spark运行时的数据读写的，当然也包含数据存储本身，在这个基础之上进行读写操作。 |
| MetricsSystem             | 一般是为了衡量系统的各种指标的度量系统。                     |
| OutputCommitCoordinator   | 确定任务是否可以把输出提到到HFDS的管理者，使用先提交者胜的策略。 |

# 二、 实例化过程

```scala
/**
  * Helper method to create a SparkEnv for a driver or an executor.
  */
private def create(
        conf: SparkConf,
        executorId: String,
        bindAddress: String,
        advertiseAddress: String,
        port: Option[Int],
        isLocal: Boolean,
        numUsableCores: Int,
        ioEncryptionKey: Option[Array[Byte]],
        listenerBus: LiveListenerBus = null,
        mockOutputCommitCoordinator: Option[OutputCommitCoordinator] = None): SparkEnv = {

    val isDriver = executorId == SparkContext.DRIVER_IDENTIFIER

    // Listener bus is only used on the driver
    if (isDriver) {
        assert(listenerBus != null, "Attempted to create driver SparkEnv with null listener bus!")
    }
    
    // "spark.authenticate.secret.driver.file" else "spark.authenticate.secret.executor.file"
    val authSecretFileConf = if (isDriver) AUTH_SECRET_FILE_DRIVER else AUTH_SECRET_FILE_EXECUTOR
    // 组件：安全管理器
    val securityManager = new SecurityManager(conf, ioEncryptionKey, authSecretFileConf)
    if (isDriver) {
        securityManager.initializeAuth()
    }

    ioEncryptionKey.foreach { _ =>
        if (!securityManager.isEncryptionEnabled()) {
            logWarning("I/O encryption enabled without RPC encryption: keys will be visible on the wire.")
        }
    }
	
    // 组件：RpvEnv
    val systemName = if (isDriver) driverSystemName else executorSystemName
    val rpcEnv = RpcEnv.create(systemName, 
                               bindAddress, 
                               advertiseAddress, 
                               port.getOrElse(-1), 
                               conf,
                               securityManager, 
                               numUsableCores, 
                               !isDriver)

    // Figure out which port RpcEnv actually bound to in case the original port is 0 or occupied.
    if (isDriver) {
        conf.set(DRIVER_PORT, rpcEnv.address.port)
    }

    // Create an instance of the class with the given name, possibly initializing it with our conf
    def instantiateClass[T](className: String): T = {
        val cls = Utils.classForName(className)
        // Look for a constructor taking a SparkConf and a boolean isDriver, then one taking just
        // SparkConf, then one taking no arguments
        try {
            cls.getConstructor(classOf[SparkConf], java.lang.Boolean.TYPE)
            .newInstance(conf, java.lang.Boolean.valueOf(isDriver))
            .asInstanceOf[T]
        } catch {
            case _: NoSuchMethodException =>
            try {
                cls.getConstructor(classOf[SparkConf]).newInstance(conf).asInstanceOf[T]
            } catch {
                case _: NoSuchMethodException =>
                cls.getConstructor().newInstance().asInstanceOf[T]
            }
        }
    }

    // Create an instance of the class named by the given SparkConf property
    // if the property is not set, possibly initializing it with our conf
    def instantiateClassFromConf[T](propertyName: ConfigEntry[String]): T = {
        instantiateClass[T](conf.get(propertyName))
    }
    
    val serializer = instantiateClassFromConf[Serializer](SERIALIZER)
    logDebug(s"Using serializer: ${serializer.getClass}")
    
	// 组件：序列化管理器
    val serializerManager = new SerializerManager(serializer, conf, ioEncryptionKey)

    val closureSerializer = new JavaSerializer(conf)

    def registerOrLookupEndpoint(name: String, endpointCreator: => RpcEndpoint):
            RpcEndpointRef = {
                if (isDriver) {
                    logInfo("Registering " + name)
                    rpcEnv.setupEndpoint(name, endpointCreator)
                } else {
                    RpcUtils.makeDriverRef(name, conf, rpcEnv)
                }
    }
	
    // 组件：广播管理器
    val broadcastManager = new BroadcastManager(isDriver, conf, securityManager)
	
    // 组件：map任务输出跟踪器
    val mapOutputTracker = if (isDriver) {
        new MapOutputTrackerMaster(conf, broadcastManager, isLocal)
    } else {
        new MapOutputTrackerWorker(conf)
    }

    // Have to assign trackerEndpoint after initialization as MapOutputTrackerEndpoint
    // requires the MapOutputTracker itself
    mapOutputTracker.trackerEndpoint = 
    		registerOrLookupEndpoint(MapOutputTracker.ENDPOINT_NAME,
                                     new MapOutputTrackerMasterEndpoint(
                                                rpcEnv, 
                                         		mapOutputTracker.asInstanceOf[MapOutputTrackerMaster], conf))
	
    // 组件：Shuffle管理器
    val shortShuffleMgrNames = Map(
        "sort" -> classOf[org.apache.spark.shuffle.sort.SortShuffleManager].getName,
        "tungsten-sort" -> classOf[org.apache.spark.shuffle.sort.SortShuffleManager].getName)
    val shuffleMgrName = conf.get(config.SHUFFLE_MANAGER)
    val shuffleMgrClass = shortShuffleMgrNames.getOrElse(shuffleMgrName.toLowerCase(Locale.ROOT), shuffleMgrName)
    val shuffleManager = instantiateClass[ShuffleManager](shuffleMgrClass)
	
    // 组件：内存管理器
    val memoryManager: MemoryManager = UnifiedMemoryManager(conf, numUsableCores)

    val blockManagerPort = if (isDriver) {
            conf.get(DRIVER_BLOCK_MANAGER_PORT)
        } else {
            conf.get(BLOCK_MANAGER_PORT)
        }

    val externalShuffleClient = if (conf.get(config.SHUFFLE_SERVICE_ENABLED)) {
            val transConf = SparkTransportConf.fromSparkConf(conf, "shuffle", numUsableCores)
            Some(new ExternalBlockStoreClient(transConf, securityManager,
                                              securityManager.isAuthenticationEnabled(), 
                                              conf.get(config.SHUFFLE_REGISTRATION_TIMEOUT)))
        } else {
            None
        }

    // Mapping from block manager id to the block manager's information.
    val blockManagerInfo = new concurrent.TrieMap[BlockManagerId, BlockManagerInfo]()
    val blockManagerMaster = new BlockManagerMaster(
        registerOrLookupEndpoint(
            BlockManagerMaster.DRIVER_ENDPOINT_NAME,
            new BlockManagerMasterEndpoint(
                rpcEnv,
                isLocal,
                conf,
                listenerBus,
                if (conf.get(config.SHUFFLE_SERVICE_FETCH_RDD_ENABLED)) {
                    externalShuffleClient
                } else {
                    None
                }, blockManagerInfo,
                mapOutputTracker.asInstanceOf[MapOutputTrackerMaster])),
        registerOrLookupEndpoint(
            BlockManagerMaster.DRIVER_HEARTBEAT_ENDPOINT_NAME,
            new BlockManagerMasterHeartbeatEndpoint(rpcEnv, isLocal, blockManagerInfo)),
        conf,
        isDriver)
	
    // 组件：块传输服务
    val blockTransferService =
            new NettyBlockTransferService(conf, 
                                          securityManager, 
                                          bindAddress, 
                                          advertiseAddress,
                                          blockManagerPort, 
                                          numUsableCores, 
                                          blockManagerMaster.driverEndpoint)

    // 组件：块管理器
    //NB: blockManager is not valid until initialize() is called later.
    val blockManager = new BlockManager(
            executorId,
            rpcEnv,
            blockManagerMaster,
            serializerManager,
            conf,
            memoryManager,
            mapOutputTracker,
            shuffleManager,
            blockTransferService,
            securityManager,
            externalShuffleClient)
	
    // 度量系统
    val metricsSystem = if (isDriver) {
        // Don't start metrics system right now for Driver.
        // We need to wait for the task scheduler to give us an app ID.
        // Then we can start the metrics system.
        MetricsSystem.createMetricsSystem(MetricsSystemInstances.DRIVER, conf, securityManager)
    } else {
        // We need to set the executor ID before the MetricsSystem is created because sources and
        // sinks specified in the metrics configuration file will want to incorporate this executor's
        // ID into the metrics they report.
        conf.set(EXECUTOR_ID, executorId)
        val ms = MetricsSystem.createMetricsSystem(MetricsSystemInstances.EXECUTOR, conf,
                                                   securityManager)
        ms.start(conf.get(METRICS_STATIC_SOURCES_ENABLED))
        ms
    }
	
    // 输出提交协调器
    val outputCommitCoordinator = mockOutputCommitCoordinator.getOrElse {
        new OutputCommitCoordinator(conf, isDriver)
    }
    val outputCommitCoordinatorRef = 
    		registerOrLookupEndpoint("OutputCommitCoordinator",
                                     new OutputCommitCoordinatorEndpoint(rpcEnv, outputCommitCoordinator))
    
    outputCommitCoordinator.coordinatorRef = Some(outputCommitCoordinatorRef)

    val envInstance = new SparkEnv(
        executorId,
        rpcEnv,
        serializer,
        closureSerializer,
        serializerManager,
        mapOutputTracker,
        shuffleManager,
        broadcastManager,
        blockManager,
        securityManager,
        metricsSystem,
        memoryManager,
        outputCommitCoordinator,
        conf)

    // Add a reference to tmp dir created by driver, we will delete this tmp dir when stop() is
    // called, and we only need to do it for driver. Because driver may run as a service, and if we
    // don't delete this tmp dir when sc is stopped, then will create too many tmp dirs.
    if (isDriver) {
        val sparkFilesDir = Utils.createTempDir(Utils.getLocalDir(conf), "userFiles").getAbsolutePath
        envInstance.driverTmpDir = Some(sparkFilesDir)
    }

    envInstance
}
```

# 三、 安全管理器`SecurityManager`

`SecurityManager`负责安全相关事情。初始化安全管理器的代码如下：

```scala
// "spark.authenticate.secret.driver.file" else "spark.authenticate.secret.executor.file"
val authSecretFileConf = if (isDriver) AUTH_SECRET_FILE_DRIVER else AUTH_SECRET_FILE_EXECUTOR
val securityManager = new SecurityManager(conf, ioEncryptionKey, authSecretFileConf)
if (isDriver) {
    securityManager.initializeAuth()
}

ioEncryptionKey.foreach { _ =>
    if (!securityManager.isEncryptionEnabled()) {
        logWarning("I/O encryption enabled without RPC encryption: keys will be visible on the wire.")
    }
}
```

```scala
/**
 * Spark class responsible for security.
 *
 * In general this class should be instantiated by the SparkEnv and most components
 * should access it from that. There are some cases where the SparkEnv hasn't been
 * initialized yet and this class must be instantiated directly.
 *
 * This class implements all of the configuration related to security features described
 * in the "Security" document. Please refer to that document for specific features implemented
 * here.
 */
private[spark] class SecurityManager(
    sparkConf: SparkConf,
    val ioEncryptionKey: Option[Array[Byte]] = None,
    authSecretFileConf: ConfigEntry[Option[String]] = AUTH_SECRET_FILE)
  extends Logging with SecretKeyHolder {...}

private[spark] object SecurityManager {...}
```

# 四、 RPC环境`RpcEnv`

`RpcEnv`是Spark 2.x.x出现的新组件，目的是替换之前的Akka。

```scala
// "sparkDriver" or "sparkExecutor"
val systemName = if (isDriver) driverSystemName else executorSystemName
val rpcEnv = RpcEnv.create(systemName, 
                           bindAddress, 
                           advertiseAddress, 
                           port.getOrElse(-1), 
                           conf,
                           securityManager, 
                           numUsableCores, 
                           !isDriver)

// Figure out which port RpcEnv actually bound to in case the original port is 0 or occupied.
if (isDriver) {
    conf.set(DRIVER_PORT, rpcEnv.address.port)
}
```

`RpcEnv.create()`方法代码如下：

```scala
def create(
        name: String,
        bindAddress: String,
        advertiseAddress: String,
        port: Int,
        conf: SparkConf,
        securityManager: SecurityManager,
        numUsableCores: Int,
        clientMode: Boolean): RpcEnv = {
    val config = RpcEnvConfig(conf, 
                              name, 
                              bindAddress, 
                              advertiseAddress, 
                              port, 
                              securityManager,
                              numUsableCores,
                              clientMode)
    new NettyRpcEnvFactory().create(config)
}
```

# 五、 序列化管理器`SerializerManager`

`SparkEnv`中有两个序列化的组件，分别是，创建它们的代码如下：

```scala
def instantiateClassFromConf[T](propertyName: ConfigEntry[String]): T = {
    instantiateClass[T](conf.get(propertyName))
}
def instantiateClass[T](className: String): T = {
    val cls = Utils.classForName(className)
    // Look for a constructor taking a SparkConf and a boolean isDriver, then one taking just
    // SparkConf, then one taking no arguments
    try {
        cls.getConstructor(classOf[SparkConf], java.lang.Boolean.TYPE)
            .newInstance(conf, java.lang.Boolean.valueOf(isDriver))
            .asInstanceOf[T]
    } catch {
        case _: NoSuchMethodException =>
            try {
                cls.getConstructor(classOf[SparkConf]).newInstance(conf).asInstanceOf[T]
            } catch {
                case _: NoSuchMethodException =>
                cls.getConstructor().newInstance().asInstanceOf[T]
            }
    }
}

// SERIALIZER: "spark.serializer"
val serializer = instantiateClassFromConf[Serializer](SERIALIZER)
logDebug(s"Using serializer: ${serializer.getClass}")

val serializerManager = new SerializerManager(serializer, conf, ioEncryptionKey)

val closureSerializer = new JavaSerializer(conf)
```

