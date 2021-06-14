> RDD，全称Resilient Distributed Datasets，弹性分布式数据集，是Spark中最基本的数据抽象，代表可并行操作元素的不可变分区集合。RDD的设计思想可参考论文：http://people.csail.mit.edu/matei/papers/2012/nsdi_spark.pdf



RDD类是`org.apache.spark.rdd.RDD`：

```scala
package org.apache.spark.rdd

/**
 * A Resilient Distributed Dataset (RDD), the basic abstraction in Spark. Represents an immutable,
 * partitioned collection of elements that can be operated on in parallel. 
 */
abstract class RDD[T: ClassTag](
    @transient private var _sc: SparkContext,
  	// 存储当前RDD的依赖
    @transient private var deps: Seq[Dependency[_]]) extends Serializable with Logging {
  
  @DeveloperApi
  def compute(split: Partition, context: TaskContext): Iterator[T]
  
 	// 分区计算器
  @transient val partitioner: Option[Partitioner] = None
  
  /** A friendly name for this RDD */
  @transient var name: String = _
  
  // 与deps相同，但是可以被序列化
  @volatile private var dependencies_ : Seq[Dependency[_]] = _
  @volatile @transient private var legacyDependencies: WeakReference[Seq[Dependency[_]]] = _
  
  // 存储当前RDD所有的分区
  @volatile @transient private var partitions_ : Array[Partition] = _
  
  // 存储级别
  private var storageLevel: StorageLevel = StorageLevel.NONE
  @transient private var resourceProfile: Option[ResourceProfile] = None

  /** User code that created this RDD (e.g. `textFile`, `parallelize`). */
  @transient private[spark] val creationSite = sc.getCallSite()
	
  // 操作作用域
  @transient private[spark] val scope: Option[RDDOperationScope] = {
    Option(sc.getLocalProperty(SparkContext.RDD_SCOPE_KEY)).map(RDDOperationScope.fromJson)
  }
  
  private[spark] var checkpointData: Option[RDDCheckpointData[T]] = None

  def map[U: ClassTag](f: T => U): RDD[U] = withScope {
    val cleanF = sc.clean(f)
    new MapPartitionsRDD[U, T](this, (_, _, iter) => iter.map(cleanF))
  }
  
  def flatMap[U: ClassTag](f: T => TraversableOnce[U]): RDD[U] = withScope {
    val cleanF = sc.clean(f)
    new MapPartitionsRDD[U, T](this, (_, _, iter) => iter.flatMap(cleanF))
  }
  
  def filter(f: T => Boolean): RDD[T] = withScope {
    val cleanF = sc.clean(f)
    new MapPartitionsRDD[T, T](
      this,
      (_, _, iter) => iter.filter(cleanF),
      preservesPartitioning = true)
  }
  
  ... 
}
  

/**
 * Defines implicit functions that provide extra functionalities on RDDs of specific types.
 */
object RDD {

  private[spark] val CHECKPOINT_ALL_MARKED_ANCESTORS = "spark.checkpoint.checkpointAllMarkedAncestors"

  implicit def rddToPairRDDFunctions[K, V](rdd: RDD[(K, V)])
    (implicit kt: ClassTag[K], vt: ClassTag[V], ord: Ordering[K] = null): PairRDDFunctions[K, V] = {
    new PairRDDFunctions(rdd)
  }

  implicit def rddToAsyncRDDActions[T: ClassTag](rdd: RDD[T]): AsyncRDDActions[T] = {
    new AsyncRDDActions(rdd)
  }

  implicit def rddToSequenceFileRDDFunctions[K, V](rdd: RDD[(K, V)])
      (implicit kt: ClassTag[K], vt: ClassTag[V],
                keyWritableFactory: WritableFactory[K],
                valueWritableFactory: WritableFactory[V])
    : SequenceFileRDDFunctions[K, V] = {
    implicit val keyConverter = keyWritableFactory.convert
    implicit val valueConverter = valueWritableFactory.convert
    new SequenceFileRDDFunctions(rdd,
      keyWritableFactory.writableClass(kt), valueWritableFactory.writableClass(vt))
  }

  implicit def rddToOrderedRDDFunctions[K : Ordering : ClassTag, V: ClassTag](rdd: RDD[(K, V)])
    : OrderedRDDFunctions[K, V, (K, V)] = {
    new OrderedRDDFunctions[K, V, (K, V)](rdd)
  }

  implicit def doubleRDDToDoubleRDDFunctions(rdd: RDD[Double]): DoubleRDDFunctions = {
    new DoubleRDDFunctions(rdd)
  }

  implicit def numericRDDToDoubleRDDFunctions[T](rdd: RDD[T])(implicit num: Numeric[T])
    : DoubleRDDFunctions = {
    new DoubleRDDFunctions(rdd.map(x => num.toDouble(x)))
  }
}
```

所有的RDD都支持一些基础操作，如`map`/`flatMap`/`filter`/`persist`。额外的，

- `org.apache.spark.rdd.PairRDDFunctions`支持一些kv对的RDD操作，如`groupByKey`/`join`。
- `org.apache.spark.rdd.DoubleRDDFunctions`支持一些`Double`的RDD操作
- `org.apache.spark.rdd.SequenceFileRDDFunctions`支持一些可以保存为`SequenceFile`的RDD的操作。



RDD由5个主要属性：

- A list of partitions

  对于RDD来说，每个partition都会被一个计算任务处理，会决定并行计算的粒度。用户可以在创建RDD时指定RDD的分片个数，如果没有指定，那么就会采用默认值（程序所分配到的CPU Core的数目）。

- A function for computing each split

- A list of dependencies on other RDDs

- Optionally, a Partitioner for key-value RDDs

  当前Spark中实现了两种类型的分片函数，一个是基于哈希的`HashPartitioner`，另外一个是基于范围的`RangePartitioner`。只有对于key-value的RDD，才会有Partitioner，非key-value的RDD的Parititioner是None。Partitioner函数不但决定了RDD本身的分片数量，也决定了parent RDD Shuffle输出时的分片数量。

- Optionally, a list of preferred locations to compute each split on

  储存取每个Partition的优先位置（preferred location）的list。对于一个HDFS文件来说，这个列表保存的就是每个Partition所在的块的位置。按照“移动数据不如移动计算”的理念，Spark在进行任务调度的时候，会尽可能地将计算任务分配到其所要处理数据块的存储位置。

RDD可以由以下几种方式得来：

- 读取文件生成
- 通过并行化方式创建
- 由其他RDD转化而来

一些重要的继承类：

```
RDD (org.apache.spark.rdd)
    BaseStateStoreRDD (org.apache.spark.sql.execution.streaming.state)
        ReadStateStoreRDD (org.apache.spark.sql.execution.streaming.state)
        StateStoreRDD (org.apache.spark.sql.execution.streaming.state)
    BlockRDD (org.apache.spark.rdd)
        WriteAheadLogBackedBlockRDD (org.apache.spark.streaming.rdd)
    CartesianRDD (org.apache.spark.rdd)
        UnsafeCartesianRDD (org.apache.spark.sql.execution.joins)
    CheckpointRDD (org.apache.spark.rdd)
        LocalCheckpointRDD (org.apache.spark.rdd)
        ReliableCheckpointRDD (org.apache.spark.rdd)
    CoalescedRDD (org.apache.spark.rdd)
    CoGroupedRDD (org.apache.spark.rdd)
    ContinuousDataSourceRDD (org.apache.spark.sql.execution.streaming.continuous)
    ContinuousWriteRDD (org.apache.spark.sql.execution.streaming.continuous)
    CustomShuffledRDD (org.apache.spark.scheduler)
    CyclicalDependencyRDD in RDDSuite (org.apache.spark.rdd)
    DataSourceRDD (org.apache.spark.sql.execution.datasources.v2)
    EmptyRDD (org.apache.spark.rdd)
    EmptyRDDWithPartitions in CoalesceExec$ (org.apache.spark.sql.execution)
    FatPairRDD (org.apache.spark)
    FatRDD (org.apache.spark)
    FetchFailureHidingRDD (org.apache.spark.executor)
    FetchFailureThrowingRDD (org.apache.spark.executor)
    FileScanRDD (org.apache.spark.sql.execution.datasources)
    HadoopMapPartitionsWithSplitRDD in HadoopRDD$ (org.apache.spark.rdd)
    HadoopRDD (org.apache.spark.rdd)
    JdbcRDD (org.apache.spark.rdd)
    JDBCRDD (org.apache.spark.sql.execution.datasources.jdbc)
    KafkaRDD (org.apache.spark.streaming.kafka010)
    KafkaSourceRDD (org.apache.spark.sql.kafka010)
    LocationPrefRDD (org.apache.spark.rdd)
    MapPartitionsRDD (org.apache.spark.rdd)
    MapWithStateRDD (org.apache.spark.streaming.rdd)
    MockRDD (org.apache.spark.scheduler)
        MockRDDWithLocalityPrefs (org.apache.spark.scheduler)
    MyCoolRDD (org.apache.spark.rdd)
    MyRDD (org.apache.spark.scheduler)
        MyCheckpointRDD (org.apache.spark.scheduler)
    NewHadoopMapPartitionsWithSplitRDD in NewHadoopRDD$ (org.apache.spark.rdd)
    NewHadoopRDD (org.apache.spark.rdd)
        BinaryFileRDD (org.apache.spark.rdd)
        WholeTextFileRDD (org.apache.spark.rdd)
    PairwiseRDD (org.apache.spark.api.python)
    ParallelCollectionRDD (org.apache.spark.rdd)
    PartitionerAwareUnionRDD (org.apache.spark.rdd)
    PartitionPruningRDD (org.apache.spark.rdd)
    PartitionwiseSampledRDD (org.apache.spark.rdd)
    PipedRDD (org.apache.spark.rdd)
    PythonRDD (org.apache.spark.api.python)
    RandomRDD (org.apache.spark.mllib.rdd)
    RandomVectorRDD (org.apache.spark.mllib.rdd)
    ShuffledRDD (org.apache.spark.rdd)
    ShuffledRowRDD (org.apache.spark.sql.execution)
    SlidingRDD (org.apache.spark.mllib.rdd)
    SQLExecutionRDD (org.apache.spark.sql.execution)
    SubtractedRDD (org.apache.spark.rdd)
    UnionRDD (org.apache.spark.rdd)
    VertexRDD (org.apache.spark.graphx)
        VertexRDDImpl (org.apache.spark.graphx.impl)
    ZippedPartitionsBaseRDD (org.apache.spark.rdd)
        ZippedPartitionsRDD2 (org.apache.spark.rdd)
        ZippedPartitionsRDD3 (org.apache.spark.rdd)
        ZippedPartitionsRDD4 (org.apache.spark.rdd)
    ZippedWithIndexRDD (org.apache.spark.rdd)
```



