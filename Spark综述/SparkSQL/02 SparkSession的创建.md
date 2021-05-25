Spark 1.X实际上有两个Context， `SparkContext`和`SQLContext`，它们负责不同的功能。 前者专注于对Spark的中心抽象进行更细粒度的控制，而后者则专注于Spark SQL等更高级别的API。Spark的早期版本，`sparkContext`是进入Spark的切入点。RDD的创建和操作得使用`sparkContex`t提供的API；对于RDD之外的其他东西，我们需要使用其他的Context。比如对于流处理来说，我们得使用`StreamingContext`；对于SQL得使用`sqlContext`；而对于hive得使用`HiveContext`。然而DataSet和Dataframe提供的API逐渐称为新的标准API，需要一个切入点(entry point)来构建它们，所以Spark 2.0引入了一个新的切入点——`SparkSession`。

Spark 2.0引入了`SparkSession`，为用户提供了一个统一的入口来使用Spark的各项功能，实质上是`SQLContext`和`HiveContext`的组合，所以在`SQLContext`和`HiveContext`上可用的API在`SparkSession`上同样是可以使用的。另外`SparkSession`允许用户通过它调用DataFrame和Dataset相关API来编写Spark程序。

其次`SparkSession`通过工厂设计模式（factory design pattern）实现，如果没有创建`SparkSession`对象，则会实例化出一个新的`SparkSession`对象及其相关的上下文。

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

private val creationSite: CallSite = Utils.getCallSite()

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
            throw new IllegalArgumentException(
                "Unable to instantiate SparkSession with Hive support because Hive classes are not found.")
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
        val (staticConfs, otherConfs) =
        options.partition(kv => SQLConf.staticConfKeys.contains(kv._1))

        otherConfs.foreach { case (k, v) => session.sessionState.conf.setConfString(k, v) }

        if (staticConfs.nonEmpty) {
            logWarning("Using an existing SparkSession; the static sql configurations will not take" +
                       " effect.")
        }
        if (otherConfs.nonEmpty) {
            logWarning("Using an existing SparkSession; some spark core configurations may not take" +
                       " effect.")
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



## getOrCreate()

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

# 三、SparkConf

在上面的`SparkSession.builder().getOrCreate()`方法中，会首先创建一个`SparkConf`实例。`SparkConf`是Spark的配置类，内部通过`ConcurrentHashMap[String, String]`来存储spark的配置。

大部分情况下，应该通过`new SparkConf()`来创建`SparkConf`类实例，默认会载入任何以`spark.`开头的Java系统属性。也可以通过`new SparkConf(false)`来指定不加载任何外部参数。

```scala
package org.apache.spark

/**
 * Configuration for a Spark application. Used to set various Spark parameters as key-value pairs.
 *
 * Most of the time, you would create a SparkConf object with `new SparkConf()`, which will load
 * values from any `spark.*` Java system properties set in your application as well. In this case,
 * parameters you set directly on the `SparkConf` object take priority over system properties.
 *
 * For unit tests, you can also call `new SparkConf(false)` to skip loading external settings and
 * get the same configuration no matter what the system properties are.
 *
 * All setter methods in this class support chaining. For example, you can write
 * `new SparkConf().setMaster("local").setAppName("My app")`.
 *
 * @param loadDefaults whether to also load values from Java system properties
 *
 * @note Once a SparkConf object is passed to Spark, it is cloned and can no longer be modified
 * by the user. Spark does not support modifying the configuration at runtime.
 */
class SparkConf(loadDefaults: Boolean) extends Cloneable with Logging with Serializable {...}

注：Logging用来打日志，Serializable，序列化。分布式环境，SparkConf对象传来传去的，当然需要序列化。

private[spark] object SparkConf extends Logging {...}
```

`SparkConf`的配置有哪些途径获取呢？有以下三种：

- 来源于系统参数(即`System.getpropertie`s获取的属性)中以`spark.`作为前缀的属性。这一步是构建`SparkConf`实例的时候进行的。

  ```scala
  // import语句是从SparkConf类的伴生对象中导入一些东西，它们主要管理过期的、旧版本兼容的配置项，以及日志输出。
  // Scala中没有Java的静态（static）概念，类的伴生对象中维护的成员和方法就可以视为类的静态成员和静态方法。
  import SparkConf._
  
  /** Create a SparkConf that loads defaults from system properties and the classpath */
  def this() = this(true)
  
  if (loadDefaults) {
      loadFromSystemProperties(false)
  }
  
  private[spark] def loadFromSystemProperties(silent: Boolean): SparkConf = {
      // Load any spark.* system properties
      for ((key, value) <- Utils.getSystemProperties if key.startsWith("spark.")) {
      	set(key, value, silent)
      }
      this
  }
  ```

  

- 使用`SparkConf`的API（`set()`方法）进行设置

  ```scala
  /** Set a configuration variable. */
  def set(key: String, value: String): SparkConf = {
  	set(key, value, false)
  }
  
  private[spark] def set(key: String, value: String, silent: Boolean): SparkConf = {
      if (key == null) {
      	throw new NullPointerException("null key")
      }
      if (value == null) {
      	throw new NullPointerException("null value for " + key)
      }
      if (!silent) {
      	logDeprecationWarning(key)
      }
      settings.put(key, value)
      this
  }
  
  /**
     * Logs a warning message if the given config key is deprecated.
     */
  def logDeprecationWarning(key: String): Unit = {
      deprecatedConfigs.get(key).foreach { cfg =>
          logWarning(
              s"The configuration key '$key' has been deprecated as of Spark ${cfg.version} and " +
              s"may be removed in the future. ${cfg.deprecationMessage}")
          return
      }
  
      allAlternatives.get(key).foreach { case (newKey, cfg) =>
          logWarning(
              s"The configuration key '$key' has been deprecated as of Spark ${cfg.version} and " +
              s"may be removed in the future. Please use the new key '$newKey' instead.")
          return
      }
      if (key.startsWith("spark.akka") || key.startsWith("spark.ssl.akka")) {
          logWarning(
              s"The configuration key $key is not supported anymore " +
              s"because Spark doesn't use Akka since 2.0")
      }
  }
  ```

  

- 从其它`SparkConf`中克隆

  在某些情况下，同一个`SparkConf`实例中的配置信息需要被多个组件公用，而我们往往会想到的方法是将`SparkCon`f实例定义为全局变量或者通过参数传递给其他组件，但是这样会引入并发问题，虽然`settings`数据结构为`ConcurrentHashMap`是线程安全的，而且`ConcurrentHashMap`也被证明是高并发下性能表现不错的数据结构，但是存在并发就一定有性能的损失问题。`SparkConf`继承了`Cloneable`并实现了`clone()`方法，可以通过`Cloneable`提高代码的可利用性，即可以创建一个新的`SparkConf`实例b，并将a中的配置信息全部拷贝到b中，这样虽然会更耗内存，但会得到更高的性能。

  ```scala
  /** Copy this object */
  override def clone: SparkConf = {
      val cloned = new SparkConf(false)
      settings.entrySet().asScala.foreach { e =>
          cloned.set(e.getKey(), e.getValue(), true)
      }
      cloned
  }
  ```


`SparkConf`是`SparkContext`初始化的必备前提。了解了`SparkConf`，就可以分析复杂得多的`SparkContext`了。

# 三、 SparkContext

`SparkSession.builder().getOrCreate()`方法在创建完`SparkConf`后，如果是第一次创建`SparkSession`，会使用`SparkContext.getOrCreate(sparkConf)`来创建`SparkContext`实例。

`SparkContext`初始化很复杂，所以单独整理在下一节。
