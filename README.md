### 1. test MergeTable in cluster: 
```
spark-submit --class com.hyzs.spark.sql.MergeTable \
 --master yarn-cluster \
 --num-executors 3 \
 --executor-cores 2 \
 --executor-memory 3G \
 --jars lib/datanucleus-api-jdo-3.2.6.jar,lib/datanucleus-core-3.2.10.jar,lib/datanucleus-rdbms-3.2.9.jar  \
  -v  ../spark_learning-1.0.jar
```
  
### 2. test read files in cluster:
* run in yarn-cluster, read file(properties) in hdfs.
* upload properties to hdfs://hyzs/properties
* use metastore uris
```
spark-submit --class com.hyzs.spark.sql.BusinessTest \
 --master yarn-cluster \
 --num-executors 2 \
 --executor-cores 2 \
 --executor-memory 3G \
 --jars lib/datanucleus-api-jdo-3.2.6.jar,lib/datanucleus-core-3.2.10.jar,lib/datanucleus-rdbms-3.2.9.jar  \
  -v  ../spark_learning-1.0.jar  

```

### 3. test in local[2]:
* use mysql jdbc connections
```
 spark-submit --class com.hyzs.spark.sql.BusinessTest \
  --master local[4]  \
  --driver-class-path lib/datanucleus-api-jdo-3.2.6.jar,lib/datanucleus-core-3.2.10.jar,\
  lib/datanucleus-rdbms-3.2.9.jar,lib/mysql-connector-java-5.1.37.jar  \
  -v  ../spark_learning-1.0.jar

```

## time efficiency

### spark 1.5.1 :
1. exceed maximum thread pool num 128 will cause RejectedExecutionException: 36*4= 144
2. use small table join accumulated table(big)

### spark 1.6.0 :
1. all left join cause GC overhead and StackOverFlow

### 4. big data test 
1. set suitable executor parameters: num-executors, executor-memory, executor-cores(better not exceed 5), 
and configuration spark.sql.shuffle.partition, make sure reasonable data size for each task(< 50M)
2.  for 80 Vcores, 500G mem:
```
--conf spark.sql.shuffle.partitions=480 \
--driver-memory 20G \
--num-executors 22 \
--executor-memory 20G \
--executor-cores 4

```
