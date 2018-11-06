## Spark(1.6, 2.X)学习经验总结

### 1. Spark基本概念

#### Transformation and Action
- map, flatmap, filter, union, intersection, join, repartition, coalesce, reduceByKey, groupByKey, aggregateByKey
- reduce, collect, count, take, saveAsTextFile, foreach
- reduce会将结果聚集在driver端，reduceByKey在各个executor分别执行
- reduceByKey效率优于groupByKey，reduceByKey输s出端只有一个值，groupByKey汇聚了每个key的全部值，造成更多的网络和内存占用
- treeReduce, treeAggregate采用树形层级方式聚合，效率更高，在driver端的shuffle量更少。
- 主线程在driver端执行，transformation在executor执行
- collect等动作会将executor数据收集到driver
- driver端才能进行rdd操作的调用。即map过程中，不能进行其他rdd的引用操作，需要合并为整体后，再考虑后续的转换。

#### narrow dependency and wide dependency(窄依赖和宽依赖)
- byKey的变换都是宽依赖，涉及到shuffle数据的过程
- shuffle类似于hash keys into buckets
- shuffle的过程会在磁盘上产生中间临时文件

#### Job, Stage, Task
- Spark程序内的每个Action操作会产生一个新Job的提交
- Transformation会生成新的RDD，如果是宽依赖（shuffle依赖，如join，groupby等），则会划分出新的Stage；如果是窄依赖（如map，filter等）则不会
- Task是Spark程序执行时的最小单元，具体的partition数目会决定任务的task数目。
如`sc.textFile(path, n)`，读取文件时就会有n个task；如shuffle后的save操作，会根据`spark.sql.shuffle.partitions`来决定task的数目（默认值200）
- 有依赖关系的Stage会有固定先后顺序提交Task
- 不同Stage的Task的并发由TaskScheduler设置的策略决定，FIFO（默认）或FAIR

#### Master, Worker, Driver, Executor
- Master,Worker是集群启动时配置的负责分配资源和具体执行任务的节点。
- Driver和Executor是Spark应用启动时才有的，每个Spark应用有一个Driver和多个Executor，Driver会启动Spark Context，会向Master节点请求资源，Master会根据Worker节点的情况启动对应的Executor，每个Executor是Worker节点上启动的单独用于执行Task的进程。
- Standalone模式会默认在每个Worker节点启动一个Executor，当使用YARN能更有效的利用和监控集群的资源使用情况，有效调度任务，按需分配Executor个数
- Executor是spark应用在节点（worker）上启动的一个进程（process）
- 一个Worker节点可以启动多个Executor，没必要在一个节点上启动多个Worker Instance。

> https://stackoverflow.com/questions/24696777/what-is-the-relationship-between-workers-worker-instances-and-executors

#### 内存结构
- Reserved Memory.在源码中定义的，系统预留的300M内存，不建议修改。保存spark内元数据结构。
- User Memory.存储用户定义的数据结构或Spark元数据结构，默认大小：（Java堆内存-预留内存）\*25%。
- Spark Memory.Uniformed Memory，动态分配Storage和Execution的比例，默认大小：（Java堆内存-预留内存）\*75%。
- Storage主要用于缓存数据，Execution主要用于Shuffle过程。
- RDD在缓存到Storage之前，占用堆内存user memory部分, 每个Partition内数据（record）通过迭代器（Iterator）访问，同一Partition内record存储位置不一定连续。
- RDD在缓存后，将存储于堆内或堆外的Storage部分，Partition转变为Block，将占 用一段连续空间，该过程称为Unroll（展开）。

#### 共享变量（ broadcast and accumulator ）
- broadcast，每个机器（worker节点）上存储一份广播变量（只读）
- accumulator，在每个executor端可修改（add），但不可读，只限driver端可读


### 2. Shuffle过程
- shuffle是MapReduce过程的一个阶段。


#### Hash Shuffle
- spark1.2版本前使用的shuffle过程，spark2.0后移除。
- 每个mapper会根据reducer个数，遍历所有record，生成R个文件。
- 在shuffle过程中，集群最多会生成M\*R个文件，会造成文件系统效率低下及巨大的网络流量压力。
- `spark.shuffle.consolidateFiles=true`能使得每个executor shuffle write在同一个文件，core\*R个临时文件，不会因reducer个数造成大量临时文件。
- 不需要排序

#### Sort Shuffle
- spark1.2后默认使用的shuffle过程。
- 在mapper端将文件根据reducer id加上索引并排序，生成整个临时文件；
- 每个map生成M个临时文件（还有M个index文件）；
- 排序后的文件能直接传输整块数据块给每个需要数据的reducer；
- 如果reducer个数不多（不超过`spark.shuffle.sort.bypassMergeThreshold`），将跳过map端的临时文件合并和排序；
- 使用AppendOnlyMap结构来存储map端输出在内存的数据
- 如果没有足够的内存来存储map端输出的内容，将使用本地磁盘；
- 参数`spark.shuffle.spill `能控制开启或关闭spill（默认开启）；
- 减少map端生成的碎片文件数量；
- 减少随机IO操作，大部分是有序读写；
- 排序比哈希操作效率低；

#### Tungsten Sort(Unsafe Shuffle)
- spark1.4后可设置`spark.shuffle.manager = tungsten-sort`来优化shuffle过程；
- 直接操作二进制文件，不需要反序列化；
- 使用ShuffleExternalSorter排序；
- spill效率更高；
- 不稳定


> https://0x0fff.com/spark-architecture-shuffle/

### 3. SparkSQL join类型

#### Shuffle Hash Join
- 中表join大表时，可filter过滤大表部分数据，然后进行join操作，可有效降低shuffle量
- left join不会减少大表shuffle数据量

#### Broadcast Hash Join
- 大表join小表时，把小表作为广播变量广播到所有executor，这样可以避免大表的shuffle


#### Sort Merge Join

### 4. Spark常用优化操作

- 进行多表、大表的join操作时，尽量在合表前将表以连接key进行repartition操作，这样可以触发sort merge join
- 使用cache或persist进行缓存时，尽量在执行cache后或调用被缓存数据前执行一次action（如first），以保证缓存在后面操作中生效
- 使用mapPartitions来优化函数内引用的外部对象(不可序列化)，如connection或其他工具lib
```
  data.foreachPartition({ records =>
  	val connection = new DatabaseConnection()
  	records.foreach( record => connection. // do something )
  })

```
- spark中使用Jackson的Json库时，可使用广播变量(broadcast variables)，或使用mapPartitions函数，对于每个partition创建一个mapper对象
```
  val mapper = new ObjectMapper()
  mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
  // NOTE: not serializable
  // mapper.registerModule(DefaultScalaModule)
  val broadMapper: Broadcast[ObjectMapper] = sc.broadcast(mapper)
```

> https://github.com/databricks/learning-spark

### 5. Spark程序调试

#### 内存的调试（OOM）

例如一个5\*100G的集群，考虑到driver和executor的1+n配置，可以设置19个executor，平均每个节点4个executor。理论上可用内存为25G，executor memory控制堆内存，考虑到`spark.yarn.executor.memory.overhead = max(384M, .07* spark.executor.memory)`，即需要0.07\*executor memory的空间作为堆外内存，分配给executor memory的内存不能超过23G。实际情况中，节点一般会有其他进程的内存占用，因此最后设置的executor memory=22G。  
在分配内存时最好用free来查看系统实际可用内存情况。 如果没有考虑到集群的实际情况，分配spark程序的内存大于节点实际可用内存，则也会造成系统kill掉executor的进程，导致executor lost。

Spark程序的executor-cores并不是系统core的概念，是spark程序使用的虚拟核心（v-cores），代表executor的并发执行task的速度。例如在设置`--num-executors 19 --executor-cores 3`时，程序实际task并发数是19\*3=57

经验法则: executor-cores最好不超过5(过大并发数会造成IO和内存压力，容易导致任务失败)

> https://community.hortonworks.com/articles/42803/spark-on-yarn-executor-resource-allocation-optimiz.html
> https://blog.cloudera.com/blog/2015/03/how-to-tune-your-apache-spark-jobs-part-2/

#### 控制shuffle过程的分块数

可通过设置`spark.sql.shuffle.partitions`或每次手动执行repartition函数来控制shuffle过程的分块数。spark的速度瓶颈一般在产生shuffle的阶段。一些重要操作如join的shuffle过程难以避免，在大数据场景中的合表过程中，设置合适的partition数目可能避免分块过大（2G）、执行效率低下或数据倾斜的问题。partition数目的设置决定了task的数目，越大的partition数目会使task并发执行时处理的数据量更少，执行时间更短，同时如果执行时间过短（<100ms），则效率更低，因此调试时需要根据数据和集群环境实际情况来配置。

经验法则: 每个partition在128M左右（可能和hadoop block size设置有关），每个task执行时间大于100ms

> https://robertovitillo.com/2015/06/30/spark-best-practices/

#### 数据倾斜

> https://www.slideshare.net/cloudera/top-5-mistakes-to-avoid-when-writing-apache-spark-applications
> https://spark.apache.org/docs/latest/tuning.html#determining-memory-consumption

#### GC调试

> https://databricks.com/blog/2015/05/28/tuning-java-garbage-collection-for-spark-applications.htmlspark learning