### 1. test MergeTable in cluster: 
```
spark-submit --class com.hyzs.MergeTable \
 --master yarn-cluster \
 --num-executors 2 \
 --executor-cores 2 \
 --executor-memory 3G \
 --jars lib/datanucleus-api-jdo-3.2.6.jar,lib/datanucleus-core-3.2.10.jar,lib/datanucleus-rdbms-3.2.9.jar  \
  -v  ../convertLibSVM-0.0.1-jar-with-dependencies.jar
```
  
### 2. test read files in cluster:
* run in yarn-cluster, read file(properties) in hdfs.
* upload properties to hdfs://hyzs/properties
* use metastore uris
```
spark-submit --class com.hyzs.BusinessTest \
 --master yarn-cluster \
 --num-executors 2 \
 --executor-cores 2 \
 --executor-memory 3G \
 --jars lib/datanucleus-api-jdo-3.2.6.jar,lib/datanucleus-core-3.2.10.jar,lib/datanucleus-rdbms-3.2.9.jar  \
  -v  ../convertLibSVM-0.0.1-jar-with-dependencies.jar  test1

```

### 3. test in local[2]:
* use mysql jdbc connections
```$xslt
 spark-submit --class com.hyzs.BusinessTest \
  --master local[4]  \
  --driver-class-path lib/datanucleus-api-jdo-3.2.6.jar,lib/datanucleus-core-3.2.10.jar,\
  lib/datanucleus-rdbms-3.2.9.jar,lib/mysql-connector-java-5.1.37.jar  \
  -v  ../convertLibSVM-0.2.jar

```