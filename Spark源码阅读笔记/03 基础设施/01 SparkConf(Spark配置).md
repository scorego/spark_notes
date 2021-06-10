`SparkConf`是Spark的配置类，内部通过`ConcurrentHashMap[String, String]`来存储spark的配置。

大部分情况下，应该通过`new SparkConf()`来创建`SparkConf`类实例，这样默认会载入任何以`spark.`开头的Java系统属性。也可以通过`new SparkConf(false)`来明确指定不加载任何外部参数。

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

private[spark] object SparkConf extends Logging {...}
```

# 1. 构造器

- `new SparkConf()`

  新建一个`SparkConf`类实例，并从系统属性中加载spark配置。

- `new SparkConf(loadDefaults: Boolean)`

  新建一个`SparkConf`类实例，参数`loadDefaults`为`true`表示从系统属性中加载spark配置，`false`表示不从系统属性中加载spark配置。spark配置就是key以`spark.`开头的配置。

# 2. 参数获取途径

`SparkConf`的配置有哪些途径获取呢？有以下三种：

- 来源于系统参数(即`System.getproperties`获取的属性)中以`spark.`作为前缀的属性。这一步是构造器构建`SparkConf`实例的时候进行的。

  ```scala
  class SparkConf(loadDefaults: Boolean) extends Cloneable with Logging with Serializable {
  
      // import语句是从SparkConf类的伴生对象中导入一些东西，它们主要管理过期的、旧版本兼容的配置项，以及日志输出。
      // Scala中没有Java的静态（static）概念，类的伴生对象中维护的成员和方法就可以视为类的静态成员和静态方法。
      import SparkConf._
  
      // 无参构造器默认的参数loadDefaults为true，表示从系统属性中加载spark配置
      def this() = this(true)
  
      if (loadDefaults) {
          loadFromSystemProperties(false)
      }
  
      private[spark] def loadFromSystemProperties(silent: Boolean): SparkConf = {
          for ((key, value) <- Utils.getSystemProperties if key.startsWith("spark.")) {
              set(key, value, silent)
          }
          this
      }
      
      ...
  }
  ```

- 使用`SparkConf`的API(`set()`方法)进行设置

  ```scala
  /** Set a configuration variable. */
  def set(key: String, value: String): SparkConf = {
  	set(key, value, false)
  }
  
  // silent为true表示静默加载，遇到deprecated的key也不会输出warning log
  private[spark] def set(key: String, value: String, silent: Boolean): SparkConf = {
      if (key == null) { throw new NullPointerException("null key")}
      if (value == null) { throw new NullPointerException("null value for " + key) }
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
          logWarning(s"The configuration key $key is not supported anymore because " +
                     s"Spark doesn't use Akka since 2.0")
      }
  }
  ```
  
- 从其它`SparkConf`中克隆

  在某些情况下，同一个`SparkConf`实例中的配置信息需要被多个组件公用，而将`SparkConf`实例定义为全局变量或者通过参数传递给其他组件会引入并发问题。虽然`settings`数据结构为`ConcurrentHashMap`是线程安全的，而且`ConcurrentHashMap`也被证明是高并发下性能表现不错的数据结构，但是存在并发就一定有性能的损失问题。`SparkConf`继承了`Cloneable`并实现了`clone()`方法，可以通过`Cloneable`提高代码的可利用性，即可以创建一个新的`SparkConf`实例b，并将a中的配置信息全部拷贝到b中，这样虽然会更耗存储资源，但会得到更高的性能。

  ```scala
  /** Copy this object */
  override def clone: SparkConf = {
      val cloned = new SparkConf(false)
      settings.entrySet().asScala.foreach { e => cloned.set(e.getKey(), e.getValue(), true) }
      cloned
  }
  ```
