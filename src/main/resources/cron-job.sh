#!/bin/bash

# crontab -e[-l] add : * */1 * * * bash .../cron-job.sh

java --classpath .../HiveToLibsvm.jar ConvertData all &> /home/hyzs/logs/log_$(date "+%Y-%m-%d-%H").log
#spark-submit .../jar convertLibsvm /home/hyzs/result_$(date "+%Y-%m-%d") >> /home/hyzs/logs/log_$(date "+%Y-%m-%d").log

data_type=train
table_name=hyzs.test_cmis
result_path=/hyzs/hjz/output/test_cmis/
#train不需指定obj文件，valid及test需指定obj文件
obj_file=
#train及valid需指定label表，test不需指定label
label_table=hyzs.bx_cust_no_label


/usr/lib/discover/bin/spark-submit \
--class huacloud.convertLibSVM.ConvertLibSvmFF \
--master yarn-client \
--driver-cores 2 \
--executor-cores 2 \
--num-executors 8 \
--driver-memory 4G \
--executor-memory 8G \
--conf 'spark.driver.maxResultSize=2048' \
--conf 'spark.driver.extraJavaOptions=-XX:PermSize=512M -XX:MaxPermSize=1024M' \
--jars /usr/lib/discover/lib/datanucleus-api-jdo-3.2.6.jar,/usr/lib/discover/lib/datanucleus-core-3.2.10.jar,/usr/lib/discover/lib/datanucleus-rdbms-3.2.9.jar,/usr/share/java/mysql-connector-java.jar \
-v /home/hyzs/1104/convertLibSVM-0.0.1-SNAPSHOT-jar-with-dependencies.jar \
"{'data_type':'"$data_type"','table_name':'"$table_name"','result_path':'"$result_path"','client_no':'cust_no_label__client_no','obj_file':'"$obj_file"'}" \
>> /home/hyzs/logs/log_$(date "+%Y-%m-%d-%H").log 2>&1

/usr/lib/discover/bin/spark-submit \
--class huacloud.convertLibSVM.LibsvmffToLimsvm \
--master yarn-cluster \
--driver-cores 2 \
--executor-cores 2 \
--num-executors 4 \
--driver-memory 4G \
--executor-memory 8G \
--conf 'spark.driver.extraJavaOptions=-XX:PermSize=512M -XX:MaxPermSize=1024M' \
--jars /usr/lib/discover/lib/datanucleus-api-jdo-3.2.6.jar,/usr/lib/discover/lib/datanucleus-core-3.2.10.jar,/usr/lib/discover/lib/datanucleus-rdbms-3.2.9.jar,/usr/share/java/mysql-connector-java.jar \
-v /home/hyzs/1029/convertLibSVM-0.0.1-SNAPSHOT-jar-with-dependencies.jar \
"{'data_type':'"$data_type"','libsvmff_file':'"$result_path$table_name".libsvmff','label_table':'"$label_table"','result_path':'"$result_path"','client_no_colume':'client_no','label_colume':'label'}" \
>> /home/hyzs/logs/log_$(date "+%Y-%m-%d-%H").log 2>&1