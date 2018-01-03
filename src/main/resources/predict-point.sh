#!/bin/bash

## generate hive table from test
# make sure pinlist file in current directory
# file should be one pin one row
# TODO: change file correspondingly
file=pinlist.txt

fileName=${file%.*}
today=$(date +'%Y%m%d')
filePath=/hyzs/files/${today}/
hdfs dfs -mkdir ${filePath}
hdfs dfs -rm -r ${filePath}${fileName}
hdfs dfs -mkdir ${filePath}${fileName}
hdfs dfs -put ./${file} ${filePath}${fileName}

## generate test data table
echo "generate pin list tables..."
/soft/spark/bin/spark-shell  --master yarn-client \
--driver-memory 20G --num-executors 22 --executor-memory 20G --executor-cores 3 \
--jars /soft/spark/lib/hadoop-lzo-0.4.20-SNAPSHOT.jar \
--driver-class-path /export/home/hcfruser/prop \
--conf "spark.predictJob.fileAbsPath=${filePath}${fileName}"  \
--conf "spark.predictJob.fileName=${fileName}" < PredictPoint.scala

## convert table to libsvm
#train不需指定obj文件，valid及test需指定obj文件
#obj_file=/hyzs/hjz/output/1206/consume_train/hyzs.result_consume_train_1204.obj
#obj_file=/hyzs/hjz/output/1206/value_train/hyzs.result_value_train_1205.obj
#obj_file=/hyzs/hjz/output/1212/risk_train/hyzs.result_risk_train.obj
echo "submit convert jobs..."
data_type=test

## consume point libsvm
consume_path=/hyzs/output/point/${fileName}_consume/
table_name=hyzs.result_consume_test_${fileName}
obj_file=/hyzs/hjz/output/1206/consume_train/hyzs.result_consume_train_1204.obj
/soft/spark/bin/spark-submit \
--class huacloud.convertLibSVM.ConvertLibSvmFF \
--master yarn-client \
--executor-cores 4 \
--num-executors 22 \
--driver-memory 20G \
--executor-memory 20G \
--conf 'spark.driver.maxResultSize=2048' \
--jars /soft/spark/lib/hadoop-lzo-0.4.20-SNAPSHOT.jar \
-v /export/home/hcfruser/convertLibSVM-0.0.1-SNAPSHOT-jar-with-dependencies.jar \
"{'data_type':'"$data_type"','table_name':'"$table_name"','result_path':'"${consume_path}"','client_no':'user_id','obj_file':'"$obj_file"'}"
echo "sleep 3 secs..."
sleep 3
/soft/spark/bin/spark-submit \
--class huacloud.convertLibSVM.LibsvmffToLimsvm \
--master yarn-client \
--executor-cores 4 \
--num-executors 22 \
--driver-memory 20G \
--executor-memory 20G \
--jars /soft/spark/lib/hadoop-lzo-0.4.20-SNAPSHOT.jar \
-v /export/home/hcfruser/convertLibSVM-0.0.1-SNAPSHOT-jar-with-dependencies.jar \
"{'data_type':'"$data_type"','libsvmff_file':'"${consume_path}${table_name}".libsvmff','label_table':'"$label_table"','result_path':'"$consume_path"','client_no_colume':'user_id','label_colume':'label'}"
echo "sleep 3 secs..."
sleep 3

## value point libsvm
value_path=/hyzs/output/point/${fileName}_value/
table_name=hyzs.result_value_test_${fileName}
obj_file=/hyzs/hjz/output/1206/value_train/hyzs.result_value_train_1205.obj
/soft/spark/bin/spark-submit \
--class huacloud.convertLibSVM.ConvertLibSvmFF \
--master yarn-client \
--executor-cores 4 \
--num-executors 22 \
--driver-memory 20G \
--executor-memory 20G \
--conf 'spark.driver.maxResultSize=2048' \
--jars /soft/spark/lib/hadoop-lzo-0.4.20-SNAPSHOT.jar \
-v /export/home/hcfruser/convertLibSVM-0.0.1-SNAPSHOT-jar-with-dependencies.jar \
"{'data_type':'"$data_type"','table_name':'"$table_name"','result_path':'"${value_path}"','client_no':'user_id','obj_file':'"$obj_file"'}"
echo "sleep 3 secs..."
sleep 3
/soft/spark/bin/spark-submit \
--class huacloud.convertLibSVM.LibsvmffToLimsvm \
--master yarn-client \
--executor-cores 4 \
--num-executors 22 \
--driver-memory 20G \
--executor-memory 20G \
--jars /soft/spark/lib/hadoop-lzo-0.4.20-SNAPSHOT.jar \
-v /export/home/hcfruser/convertLibSVM-0.0.1-SNAPSHOT-jar-with-dependencies.jar \
"{'data_type':'"$data_type"','libsvmff_file':'"${value_path}${table_name}".libsvmff','label_table':'"$label_table"','result_path':'"$value_path"','client_no_colume':'user_id','label_colume':'label'}"
echo "sleep 3 secs..."
sleep 3

## risk point libsvm
risk_path=/hyzs/output/point/${fileName}_risk/
table_name=hyzs.result_risk_test_${fileName}
obj_file=/hyzs/hjz/output/1212/risk_train/hyzs.result_risk_train.obj
/soft/spark/bin/spark-submit \
--class huacloud.convertLibSVM.ConvertLibSvmFF \
--master yarn-client \
--executor-cores 4 \
--num-executors 22 \
--driver-memory 20G \
--executor-memory 20G \
--conf 'spark.driver.maxResultSize=2048' \
--jars /soft/spark/lib/hadoop-lzo-0.4.20-SNAPSHOT.jar \
-v /export/home/hcfruser/convertLibSVM-0.0.1-SNAPSHOT-jar-with-dependencies.jar \
"{'data_type':'"$data_type"','table_name':'"$table_name"','result_path':'"${risk_path}"','client_no':'user_id','obj_file':'"$obj_file"'}"
echo "sleep 3 secs..."
sleep 3
/soft/spark/bin/spark-submit \
--class huacloud.convertLibSVM.LibsvmffToLimsvm \
--master yarn-client \
--executor-cores 4 \
--num-executors 22 \
--driver-memory 20G \
--executor-memory 20G \
--jars /soft/spark/lib/hadoop-lzo-0.4.20-SNAPSHOT.jar \
-v /export/home/hcfruser/convertLibSVM-0.0.1-SNAPSHOT-jar-with-dependencies.jar \
"{'data_type':'"$data_type"','libsvmff_file':'"${risk_path}${table_name}".libsvmff','label_table':'"$label_table"','result_path':'"$risk_path"','client_no_colume':'user_id','label_colume':'label'}"
echo "sleep 3 secs..."
sleep 3


## predict
echo "get libsvm files..."
rm -rf ./${fileName}
mkdir ./${fileName}
hdfs dfs -get /hyzs/output/point/${fileName}* ./${fileName}
for ty in consume value risk
do
table_name=hyzs.result_${ty}_test_${fileName}
python /export/grid/01/js_run/predict.py  \
--path_test ./${fileName}/${fileName}_${ty}/${table_name}.libsvm  \
--path_pred ./${fileName}/${fileName}_${ty}/${table_name}.pred \
--path_model /export/grid/01/model_${ty}/result/${ty}.model
python /export/grid/01/concat_index.py \
 ./${fileName}/${fileName}_${ty}/${table_name}.index \
 ./${fileName}/${fileName}_${ty}/${table_name}.pred \
 ./${fileName}/${fileName}_${ty}.result
echo "finished jobs, save pred to ./${fileName}/${fileName}_${ty}.result"
done