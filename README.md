## Spark(1.6)学习经验总结

### 1. Spark基本概念

#### Job, Stage, Task
- Spark程序内的每个Action操作会产生一个新Job的提交。
- Transformation会生成新的RDD，如果是宽依赖（shuffle依赖，如join，groupby等），则会划分出新的Stage；如果是窄依赖（如map，filter等）则不会
- Task是Spark程序执行时的最小单元，具体的partition数目会决定任务的task数目。
如`sc.textFile(path, n)`，读取文件时就会有n个task；如shuffle后的save操作，会根据`spark.sql.shuffle.partitions`来决定task的数目（默认值200）。

#### Master, Worker, Driver, Executor
- Master,Worker是集群启动时配置的负责分配资源和具体执行任务的节点。
- Driver和Executor是Spark应用启动时才有的，每个Spark应用有一个Driver和多个Executor，Driver会启动Spark。 Context，会向Master节点请求资源，Master会根据Worker节点的情况启动对应的Executor，每个Executor是Worker节点上启动的单独用于执行Task的进程。
- Standalone模式会默认在每个Worker节点启动一个Executor，当使用YARN能更有效的利用和监控集群的资源使用情况，有效调度任务，按需分配Executor个数
- 一个Worker节点可以启动多个Executor，没必要在一个节点上启动多个Worker Instance。

> https://stackoverflow.com/questions/24696777/what-is-the-relationship-between-workers-worker-instances-and-executors

### 2. Spark常用操作

- 进行多表、大表的join操作时，尽量在合表前将表以连接key进行repartition操作，这样可以触发sort merge join。
- join操作前，至少将起始表进行repartition操作。
- 使用cache或persist进行缓存时，尽量在执行cache后或调用被缓存数据前执行一次action（如first），以保证缓存在后面操作中生效。
- 使用mapPartitions来优化函数内引用的外部对象(不可序列化)，每个partition内创建一次对象，如connection或其他工具lib的对象。
```
  data.foreachPartition({ records =>
  	val connection = new DatabaseConnection()
  	records.foreach( record => connection. // do something )
  })

```
- spark中使用Jackson的Json库时，可使用`SparkContext.broadcast`来包装广播变量(broadcast variables)。
```
  val mapper = new ObjectMapper()
  mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
  // NOTE: not serializable
  // mapper.registerModule(DefaultScalaModule)
  val broadMapper: Broadcast[ObjectMapper] = sc.broadcast(mapper)
```
- 通常情况下，`RDD.coalesce`是窄依赖（shuffle默认为false），因此在将rdd多数分块减少到少数时，使用coalesce比直接使用repartition效率更高。
- `RDD.saveAsTextFile`通常会根据partition数生成多个文件，**在rdd较小的情况下**，可使用`rdd.coalesce(1)`来合并成一个文件，但**大数据场景**下不建议使用这个方法，该方法会把rdd的所有分块全部汇集到driver节点，很有可能造成程序崩溃。如要合并多个文件，可考虑使用hadoop提供的`FileUtil.copyMerge`方法。

> https://stackoverflow.com/questions/31674530/write-single-csv-file-using-spark-csv/41785085#41785085
> https://github.com/databricks/learning-spark

### 3. Spark程序调试

#### 内存的调试（OOM）

例如一个5\*100G的集群，考虑到driver和executor的1+n配置，可以设置19个executor，平均每个节点4个executor。理论上可用内存为25G，executor memory控制堆内存，考虑到`spark.yarn.executor.memory.overhead = max(384M, .07* spark.executor.memory)`，即需要0.07\*executor memory的空间作为堆外内存，分配给executor memory的内存不能超过23G。实际情况中，节点一般会有其他进程的内存占用，因此最后设置的executor memory=22G。  
在分配内存时最好监测下系统实际可用内存情况。如果没有考虑到集群的实际情况，分配spark程序executor的内存等于节点最大可用内存，则也会造成系统kill掉executor的进程，导致executor lost。

Spark程序的executor-cores并不是系统core的概念，是spark程序使用的虚拟核心（v-cores），代表executor的并发执行task的速度。例如在设置`--num-executors 19 --executor-cores 3`时，程序实际task并发数是19\*3=57。

经验法则: executor-cores最好不超过5(过大并发数会造成IO和内存压力，容易导致任务失败)。

> https://community.hortonworks.com/articles/42803/spark-on-yarn-executor-resource-allocation-optimiz.html
> https://blog.cloudera.com/blog/2015/03/how-to-tune-your-apache-spark-jobs-part-2/

#### 控制shuffle过程的分块数

可通过设置`spark.sql.shuffle.partitions`或每次手动执行repartition函数来控制shuffle过程的分块数。spark的速度瓶颈一般在产生shuffle的阶段。一些重要操作如join的shuffle过程难以避免，在大数据场景中的合表过程中，设置合适的partition数目可能避免分块过大（2G）、执行效率低下或数据倾斜的问题。partition数目的设置决定了task的数目，越大的partition数目会使task并发执行时处理的数据量更少，执行时间更短，同时如果执行时间过短（<100ms），则会因为需处理更多task造成整体效率更低，因此调试时需要根据数据和集群环境实际情况来配置。

经验法则: 每个partition在128M左右（可能和hadoop block size设置有关），每个task执行时间大于100ms。

> https://robertovitillo.com/2015/06/30/spark-best-practices/

#### 数据倾斜

在对表进行join操作时，发现连接key为明文，加上是排序后的表，分布极其不均衡，中间产生明显的数据倾斜（大多task时间很短，少数task持续时间很长）。  
在尝试对表进行repartition，提高shuffle.partitions，提高executor内存都失败后，将key重新进行MD5转换后解决该问题。
> https://www.slideshare.net/cloudera/top-5-mistakes-to-avoid-when-writing-apache-spark-applications
> https://spark.apache.org/docs/latest/tuning.html#determining-memory-consumption

#### GC调试

> https://databricks.com/blog/2015/05/28/tuning-java-garbage-collection-for-spark-applications.html