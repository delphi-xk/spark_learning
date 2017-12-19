#!/bin/bash

## generate hive table from test
# make sure pinlist file in current directory
file=pinglist.txt
fileName=${file%.*}
today=$(date +'%Y%m%d')
filePath=/hyzs/files/${today}/
hdfs dfs -rm ${filePath}${fileName}
hdfs dfs -put ${fileName} ${filePath}

## generate test data table
echo "generate pin list tables..."
/soft/spark/bin/spark-shell  --master yarn-client \
--driver-memory 20G --num-executors 22 --executor-memory 20G --executor-cores 3 \
--jars /soft/spark/lib/hadoop-lzo-0.4.20-SNAPSHOT.jar \
--driver-class-path /export/home/hcfruser/prop \
--conf "spark.predictJob.fileAbsPath=${filePath}${fileName}"  \
--conf "spark.predictJob.file=${file}" < PredictJob.scala

## convert table to libsvm
echo "submit convert jobs..."
data_type=test
table_name=hyzs.result_risk_test_${fileName}
result_path=/hyzs/output/${today}/${fileName}/
#train不需指定obj文件，valid及test需指定obj文件  TODO: fill obj file path
obj_file=
label_table=

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
"{'data_type':'"$data_type"','table_name':'"$table_name"','result_path':'"${result_path}"','client_no':'user_id','obj_file':'"$obj_file"'}"

/soft/spark/bin/spark-submit \
--class huacloud.convertLibSVM.LibsvmffToLimsvm \
--master yarn-client \
--executor-cores 4 \
--num-executors 22 \
--driver-memory 20G \
--executor-memory 20G \
--jars /soft/spark/lib/hadoop-lzo-0.4.20-SNAPSHOT.jar \
-v /export/home/hcfruser/convertLibSVM-0.0.1-SNAPSHOT-jar-with-dependencies.jar \
"{'data_type':'"$data_type"','libsvmff_file':'"${result_path}${table_name}".libsvmff','label_table':'"$label_table"','result_path':'"$result_path"','client_no_colume':'user_id','label_colume':'label'}"


## predict
echo "get libsvm files..."
hdfs dfs -get ${result_path}
cp ./${fileName}/${result_path}${table_name}.libsvm /export/grid/01/model_risk/libsvm/${fileName}_test

python /export/grid/01/js_run/predict.py  \
--path_test /export/grid/01/model_risk/libsvm/${fileName}_test.libsvm  \
--path_pred /export/grid/01/model_risk/result/${fileName}_risk.pred \
--path_model /export/grid/01/model_risk/result/risk.model

echo "finished jobs, save pred to /export/grid/01/model_risk/result/${fileName}_risk.pred"