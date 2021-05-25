> Spark Driver用于提交用户应用程序，可以看做Spark的客户端。了解Spark Driver的初始化，有助于理解用户应用程序在客户端的处理过程。
>
> Spark Driver的初始化始终围绕着`SparkContext`的初始化。`SparkContext`可以视为Spark应用程序的发动机引擎，代表着和一个Spark群的连接，只有其初始化完毕，才能向Spark集群提交任务。



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

