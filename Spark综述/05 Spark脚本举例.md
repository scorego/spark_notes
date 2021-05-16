# 一、提交作业命令

假如我们通过`spark-submit`脚本提交一个Spark作业，命令如下：

```
./bin/spark-submit \
    --class com.example.WordCount \
    --master spark://127.0.0.1:7077 \
    --deploy-mode cluster \
    --executor-memory 4G \
    ./user-jar-0.1.1.jar \
    arg1 \
    arg2
```

实际上，`spark-submit`脚本调用的是`spark-class`脚本，`spark-class`脚本是执行`org.apache.spark.launcher.Main`主类来构建命令：

```
java -Xmx128m -cp ...jars org.apache.spark.launcher.Main \
       org.apache.spark.deploy.SparkSubmit \
       --class com.example.WordCount \
       --master spark://127.0.0.1:7077 \
       --deploy-mode cluster \
       --executor-memory 4G \
       ./user-jar-0.1.1.jar \
      arg1 \
      arg2
```

最终构建并执行的命令是:

```
java -cp ... -Duser.home=/home/work  org.apache.spark.deploy.SparkSubmit --master spark://127.0.0.1:7077 --deploy-mode cluster --class com.example.WordCount --executor-memory 4G ./user-jar-0.1.1.jar arg1 arg2
```

# 二、作业代码

```scala
package com.example
 
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
 
object WordCount {
    
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("SparkSessionWordCount")
      .master("local[*]")
      .getOrCreate()

    val lines: Dataset[String] = spark.read.textFile("input/word_count.txt")

    //添加隐式转换
    import spark.implicits._

    //Dataset只有一列，默认列名为value
    val words: Dataset[String] = lines.flatMap(_.split(" "))

    //注册视图
    words.createTempView("word_table")

    //执行sql（lazy）
    val dataFrame: DataFrame = spark.sql("select value, count(*) counts from word_table group by value order by value desc")

    //执行计算
    dataFrame.show()
  }
    
    //  或者使用SparkContext的API
    //    def main(args: Array[String]): Unit = {
    //
    //    // bin/spark-submit
    //    //    --master spark://scorego1:7077
    //    //    --class wang.javior.ch01.ScalaWordCount
    //    //    /opt/module/spark/myapp/sparklearn-1.0-SNAPSHOT.jar
    //    //    hdfs://scorego2:9000/wcinput
    //    //    hdfs://scorego2:9000/wcoutput0404
    //
    //    // 创建spark执行入口4
    //    val conf: SparkConf = new SparkConf().setAppName("SparkContextWordCount")
    //
    //    val sc: SparkContext = new SparkContext(conf)
    //
    //    // 创建RDD
    //    val lines: RDD[String] = sc.textFile(args(0))
    //    val words: RDD[String] = lines.flatMap(_.split(" "))
    //    val map: RDD[(String, Int)] = words.map((_, 1))
    //    val reducedMap: RDD[(String, Int)] = map.reduceByKey(_ + _)
    //    val res: RDD[(String, Int)] = reducedMap.sortBy(_._2, ascending = false)
    //    res.saveAsTextFile(args(1))
    //
    //    // 释放
    //    sc.stop()
    //  }
}
```

可以看到，代码的核心指出是初始化`SparkSession`实例。