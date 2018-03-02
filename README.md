## Spark(1.6)学习经验总结

### 1. Spark基本概念

#### Job, Stage, Task
- Spark程序内的每个Action操作会产生一个新Job的提交
- Transformation会生成新的RDD，如果是宽依赖（shuffle依赖，如join，groupby等），则会划分出新的Stage；如果是窄依赖（如map，filter等）则不会
- Task是Spark程序执行时的最小单元，具体的partition数目会决定任务的task数目。
如`sc.textFile(path, n)`，读取文件时就会有n个task；如shuffle后的save操作，会根据`spark.sql.shuffle.partitions`来决定task的数目（默认值200）

#### Master, Worker, Driver, Executor

### Spark常用操作


### Spark程序调试
Spark程序最常见的坑就是关于内存的调试。  

例如一个5\*100G的集群，考虑到driver和executor的1+n配置，可以设置19个executor，平均每个节点4个executor。理论上可用内存为25G，executor memory控制堆内存，考虑到`spark.yarn.executor.memory.overhead = max(384M, .07* spark.executor.memory)`，即需要0.07\*executor memory的空间作为堆外内存，分配给executor memory的内存不能超过23G。实际情况中，由于spark集群的节点上会启动一些worker deamon，会有固定的内存占用，在分配内存时最好用free来查看系统实际可用内存情况。如果使用yarn作为资源分配，也可以尽量减少起多个worker。  

Spark程序的executor-cores并不是系统core的概念，是spark程序抽象的运行核心，代表程序的并发速度。例如在设置`--num-executors 19 --executor-cores 3`时，程序实际并发运行的task数是19\*3=57






### 参考
- https://blog.cloudera.com/blog/2015/03/how-to-tune-your-apache-spark-jobs-part-2/
