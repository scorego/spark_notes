> Spark 1.X实际上有两个Context， `SparkContext`和`SQLContext`，它们负责不同的功能。 前者专注于对Spark的中心抽象进行更细粒度的控制，而后者则专注于Spark SQL等更高级别的API。Spark的早期版本中，`sparkContext`是进入Spark的切入点。RDD的创建和操作需要使用`sparkContex`t提供的API；对于RDD之外的其他东西，我们需要使用其他的Context。比如对于流处理来说，我们得使用`StreamingContext`；对于SQL得使用`SQLContext`；而对于hive得使用`HiveContext`。然而DataSet和Dataframe提供的API逐渐成为新的标准API，需要一个切入点(entry point)来构建它们，所以Spark 2.0引入了一个新的切入点——`SparkSession`。



spark 有三大引擎，Spark core、Spark SQL、Spark Streaming：Spark core的关键抽象是`SparkContext`、`RDD`；Spark SQL 的关键抽象是`SparkSession`、`DataFrame`；`SparkStreaming`的关键抽象是`StreamingContext`、`DStream`。Spark 2.0引入了`SparkSession`，为用户提供了一个统一的入口来使用Spark的各项功能，实质上是`SQLContext`和`HiveContext`的封装，所以在`SQLContext`和`HiveContext`上可用的API在`SparkSession`上同样是可以使用的。另外`SparkSession`允许用户通过它调用DataFrame和Dataset相关API来编写Spark程序。`SparkSession`主要用在 SparkSQL 中，当然也可以用在其他场合。

`SparkSession`通过工厂设计模式（factory design pattern）实现，如果没有创建`SparkSession`对象，则会实例化出一个新的`SparkSession`对象及其相关的上下文。

# 一、 SparkSession

```scala
package org.apache.spark.sql

/**
 * The entry point to programming Spark with the Dataset and DataFrame API.
 
 * @param sparkContext 				The Spark context associated with this Spark session.
 * @param existingSharedState 		If supplied, use the existing shared state
 *                            		instead of creating a new one.
 * @param parentSessionState 		If supplied, inherit all session state (i.e. temporary
 *                            		views, SQL config, UDFs etc) from parent.
 */
@Stable
class SparkSession private(
    @transient val sparkContext: SparkContext,
    @transient private val existingSharedState: Option[SharedState],
    @transient private val parentSessionState: Option[SessionState],
    @transient private[sql] val extensions: SparkSessionExtensions,
    @transient private[sql] val initialSessionOptions: Map[String, String])
  extends Serializable with Closeable with Logging {...}
  
@Stable
object SparkSession extends Logging{...}
```

## 1. 属性

`SparkSession`有以下重要属性：

实例属性：

```scala
// 在多个SparkSession之间共享的状态，包括SparkContext、缓存的数据、监听器(listener)、与外部系统交互的catalog
lazy val sharedState: SharedState = {
    existingSharedState.getOrElse(new SharedState(sparkContext, initialSessionOptions))
}

// session之间隔离的状态，包括SQL配置、临时表、注册的函数，and everything else that accepts a [[org.apache.spark.sql.internal.SQLConf]]
// 如果`parentSessionState`不是null，则该变量会是`parentSessionState`的克隆
lazy val sessionState: SessionState = {
    parentSessionState
    .map(_.clone(this))
    .getOrElse {
        val state = SparkSession.instantiateSessionState(
            SparkSession.sessionStateClassName(sparkContext.conf),
            self,
            initialSessionOptions)
        state
    }
}

// 即SQLContext，SparkSQL的上下文信息
val sqlContext: SQLContext = new SQLContext(this)

// Spark的运行时配置
lazy val conf: RuntimeConfig = new RuntimeConfig(sessionState.conf)

lazy val catalog: Catalog = new CatalogImpl(self)
```

伴生对象的属性：

```scala
// 持有当前线程激活的SparkSession
private val activeThreadSession = new InheritableThreadLocal[SparkSession]

// 持有默认的SparkSession
private val defaultSession = new AtomicReference[SparkSession]
```



# 二、 SparkSession.Builder

```scala
/**
  * Builder for [[SparkSession]].
  */
@Stable
class Builder extends Logging {

    private[this] val options = new scala.collection.mutable.HashMap[String, String]

    private[this] val extensions = new SparkSessionExtensions

    private[this] var userSuppliedContext: Option[SparkContext] = None

    private[spark] def sparkContext(sparkContext: SparkContext): Builder = synchronized {
        userSuppliedContext = Option(sparkContext)
        this
    }

    def appName(name: String): Builder = config("spark.app.name", name)


    def config(key: String, value: String): Builder = synchronized {
        options += key -> value
        this
    }
    def config(key: String, value: Long): Builder = synchronized {...}
    def config(key: String, value: Double): Builder = synchronized {...}
    def config(key: String, value: Boolean): Builder = synchronized {...}
    
    def config(conf: SparkConf): Builder = synchronized {
        conf.getAll.foreach { case (k, v) => options += k -> v }
        this
    }

    def master(master: String): Builder = config("spark.master", master)

    /**
     * Enables Hive support, including connectivity to a persistent Hive metastore, support for
     * Hive serdes, and Hive user-defined functions.
     *
     * @since 2.0.0
     */
    def enableHiveSupport(): Builder = synchronized {
        if (hiveClassesArePresent) {
            config(CATALOG_IMPLEMENTATION.key, "hive")
        } else {
            throw new IllegalArgumentException("Unable to instantiate SparkSession with Hive support because Hive classes are not found.")
        }
    }

    /**
     * Inject extensions into the [[SparkSession]]. This allows a user to add Analyzer rules,
     * Optimizer rules, Planning Strategies or a customized parser.
     *
     * @since 2.2.0
     */
    def withExtensions(f: SparkSessionExtensions => Unit): Builder = synchronized {
        f(extensions)
        this
    }

    /**
     * Gets an existing [[SparkSession]] or, if there is no existing one, creates a new
     * one based on the options set in this builder.
     *
     * This method first checks whether there is a valid thread-local SparkSession,
     * and if yes, return that one. It then checks whether there is a valid global
     * default SparkSession, and if yes, return that one. If no valid global default
     * SparkSession exists, the method creates a new SparkSession and assigns the
     * newly created SparkSession as the global default.
     *
     * In case an existing SparkSession is returned, the non-static config options specified in
     * this builder will be applied to the existing SparkSession.
     *
     * @since 2.0.0
     */
    def getOrCreate(): SparkSession = synchronized {
        ...
    }
	
    //
    private def applyModifiableSettings(session: SparkSession): Unit = {
        val (staticConfs, otherConfs) = options.partition(kv => SQLConf.staticConfKeys.contains(kv._1))
        otherConfs.foreach { case (k, v) => session.sessionState.conf.setConfString(k, v) }
        if (staticConfs.nonEmpty) {
            logWarning("Using an existing SparkSession; the static sql configurations will not take effect.")
        }
        if (otherConfs.nonEmpty) {
            logWarning("Using an existing SparkSession; some spark core configurations may not take effect.")
        }
    }
}
```

`SparkSession.Builder`类主要是用来构建`SparkSession`，它有以下属性：

- `private[this] val options = new scala.collection.mutable.HashMap[String, String]`

  用来存放构件`SparkConf`所需要的配置。

- `private[this] val extensions = new SparkSessionExtensions`

  `SparkSessionExtensions`这个类是实验性质的，主要用途是**注入扩展点**（injection points｜extension points）,包括：

  - Analyzer Rules
  - Check Analysis Rules
  - Optimizer Rules
  - Pre CBO Rules
  - Planning Strategies
  - Customized Parser
  - (External) Catalog listeners
  - Columnar Rules
  - Adaptive Query Stage Preparation Rules

  用法示例如：

  ```scala
  SparkSession.builder()
    .master("...")
    .conf("...", true)
    .withExtensions { extensions =>
      extensions.injectResolutionRule { session =>
        ...
      }
      extensions.injectParser { (session, parser) =>
        ...
      }
    }
    .getOrCreate()
    
  
  SparkSession.builder()
  	.master("...")
  	.config("spark.sql.extensions", "org.example.MyExtensions")
  	.getOrCreate()
  
  class MyExtensions extends Function1[SparkSessionExtensions, Unit] {
  	override def apply(extensions: SparkSessionExtensions): Unit = {
   		extensions.injectResolutionRule { session =>
   			...
   		}
  		extensions.injectParser { (session, parser) =>
    			...
  		}
  	}
  }
  ```

  

- `private[this] var userSuppliedContext: Option[SparkContext] = None`

  用来存放用户提供的`SparkContext`。有可能用户自己先实例化了一个`SparkContext`，可以通过 这个类的`sparkContext()`方法（这个方法只限在spark内部使用）暂时保存 用户的`sparkContext`，最后在这个类的`getOrCreate()`方法中 进行兼容处理。



## 1. getOrCreate()

`getOrCreate()`方法十分重要，代码如下：

```scala
/**
  	* Gets an existing [[SparkSession]] or, if there is no existing one, creates a new
     * one based on the options set in this builder.
     *
     * This method first checks whether there is a valid thread-local SparkSession,
     * and if yes, return that one. It then checks whether there is a valid global
     * default SparkSession, and if yes, return that one. If no valid global default
     * SparkSession exists, the method creates a new SparkSession and assigns the
     * newly created SparkSession as the global default.
     *
     * In case an existing SparkSession is returned, the non-static config options specified in
     * this builder will be applied to the existing SparkSession.
     *
     * @since 2.0.0
     */
def getOrCreate(): SparkSession = synchronized {
    // 核心步骤1： 用options的配置创建一个sparkConf
    val sparkConf = new SparkConf()
    options.foreach { case (k, v) => sparkConf.set(k, v) }

    if (!sparkConf.get(EXECUTOR_ALLOW_SPARK_CONTEXT)) {
        assertOnDriver()
    }

    // 从当前线程的active session中获取SparkSession，如果获取到则返回该SparkSession.
    var session = activeThreadSession.get()
    if ((session ne null) && !session.sparkContext.isStopped) {
        applyModifiableSettings(session)
        return session
    }

    // Global synchronization so we will only set the default session once.
    SparkSession.synchronized {
        // 如果当前线程没有active session，就从gloabl session中获取
        session = defaultSession.get()
        if ((session ne null) && !session.sparkContext.isStopped) {
            applyModifiableSettings(session)
            return session
        }

        // No active nor global default session. Create a new one.
        val sparkContext = userSuppliedContext.getOrElse {
            // set a random app name if not given.
            if (!sparkConf.contains("spark.app.name")) {
                sparkConf.setAppName(java.util.UUID.randomUUID().toString)
            }
			// 核心步骤2： 用刚才创建的sparkConf创建SparkContext
            SparkContext.getOrCreate(sparkConf)
            // Do not update `SparkConf` for existing `SparkContext`, as it's shared by all sessions.
        }

        applyExtensions(
            sparkContext.getConf.get(StaticSQLConf.SPARK_SESSION_EXTENSIONS).getOrElse(Seq.empty),
            extensions)
		
        // 核心步骤3： 用刚才创建的SparkContext创建SparkSession
        session = new SparkSession(sparkContext, None, None, extensions, options.toMap)
        
        // 把创建的session设到default session和active session
        setDefaultSession(session)
        setActiveSession(session)
        
        // listenerRegistered.set(true)
        registerContextListener(sparkContext)
    }

    return session
}
```

总的来说:

1. 用`options`中的键值对创建一个`SparkConf`。
2. 根据配置`spark.executor.allowSparkContext`来判定当前是否是driver在新建`SparkSession`。如果配置不允许executor创建`SparkContext`，则在executor上调用此方法会抛异常。
3. 如果从`activeThreadSession`(`ThreadLocal`)中能获取到激活且还未停止的`SparkSession`，那么将`options`中的键值对设置到该`SparkSession`的`sessionState`的`SQLConf`中并**返回**此`SparkSession`。
4. 如果存在默认且还未停止的`SparkSession`(`defaultSession`)，那么将`options`中的键值对设置到该`SparkSession`的`sessionState`的`SQLConf`中并**返回**此`SparkSession`。
5. 如果用户提供了`SparkContext`(存放在`userSuppliedContext`中)，则使用该`SparkContext`，否则会创建一个`SparkContext`。创建顺序如下：
   1. 当未指定`spark.app.name`配置时，会使用UUID来生成随机的名字；
   2. 调用`SparkContext`伴生对象的`getOrCreate()`方法获取或创建`SparkContext`。
6. 应用`SparkSessionExtensions`
7. 使用`SparkContext`、`SparkSessionExtensions`、`options`创建`SparkSession`。
8. 将创建的`SparkSession`设置到`activeThreadSession`和`defaultSession`中。
9. 调用该`SparkContext`的`addSparkListener()`方法，向`listenerBus`中添加匿名实现的`SparkListener`（此`SparkListener`的作用是在Application结束后清空`defaultSession`和`listenerRegistered`）。
10. 返回创建或获取的`SparkSession`。

# 二、 SparkContext

`SparkSession.builder().getOrCreate()`方法在创建完`SparkConf`后，如果是第一次创建`SparkSession`，会使用`SparkContext.getOrCreate(sparkConf)`来创建`SparkContext`实例。

`SparkContext`初始化很复杂，所以单独整理一节。

# 三、 `SparkSession`实例化过程

`getOrCreate()`方法在获取到`SparkContext`后会新建`SparkSession`实例：

```scala
session = new SparkSession(sparkContext, None, None, extensions, options.toMap)
```

`SparkSession`构造过程如下：

```scala
@Stable
class SparkSession private(
    @transient val sparkContext: SparkContext,
    @transient private val existingSharedState: Option[SharedState],
    @transient private val parentSessionState: Option[SessionState],
    @transient private[sql] val extensions: SparkSessionExtensions,
    @transient private[sql] val initialSessionOptions: Map[String, String])
  extends Serializable with Closeable with Logging {  self =>
      
      
    private[sql] val sessionUUID: String = UUID.randomUUID.toString
      
    sparkContext.assertNotStopped()

    // If there is no active SparkSession, uses the default SQL conf. Otherwise, use the session's.
    SQLConf.setSQLConfGetter(() => {
        SparkSession.getActiveSession.filterNot(_.sparkContext.isStopped)
        	.map(_.sessionState.conf)
          	.getOrElse(SQLConf.getFallbackConf)
    })
      
    lazy val sharedState: SharedState = {
    	existingSharedState.getOrElse(new SharedState(sparkContext, initialSessionOptions))
  	}
      
    lazy val sessionState: SessionState = {
        parentSessionState.map(_.clone(this))
          .getOrElse {
                val state = SparkSession.instantiateSessionState(
                      SparkSession.sessionStateClassName(sparkContext.conf),
                      self,
                      initialSessionOptions)
                state
          }
    }
      
    val sqlContext: SQLContext = new SQLContext(this)
      
    lazy val conf: RuntimeConfig = new RuntimeConfig(sessionState.conf)
    
    lazy val catalog: Catalog = new CatalogImpl(self)

	...  
}
```

