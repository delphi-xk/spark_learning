#!/bin/bash

## generate hive table from test
# make sure pinlist file in current directory
# file should be one pin one row
# TODO: change file correspondingly
file=Label6W.txt

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
--conf "spark.predictJob.fileName=${fileName}" < PredictLabel.scala

## convert table to libsvm
echo "submit convert jobs..."
for((i=1;i<=7;i++))
do
data_type=test
table_name=hyzs.pin_data_${fileName}
result_path=/hyzs/output/pin_label/${i}_test_${fileName}/
#train不需指定obj文件，valid及test需指定obj文件
obj_file=/hyzs/output/pin_label/${i}_train/hyzs.pin_data_train.obj
#train及valid需指定label表，test不需指定label
label_table=

/soft/spark/bin/spark-submit \
--class huacloud.convertLibSVM.ConvertLibSvmFF \
--master yarn-client \
--executor-cores 3 \
--num-executors 22 \
--driver-memory 20G \
--executor-memory 20G \
--jars /soft/spark/lib/hadoop-lzo-0.4.20-SNAPSHOT.jar \
-v /export/home/hcfruser/convertLibSVM-0.0.1-SNAPSHOT-jar-with-dependencies.jar \
"{'data_type':'"$data_type"','table_name':'"$table_name"','result_path':'"$result_path"','client_no':'user_id','obj_file':'"$obj_file"'}"
echo "sleep 3 seconds..."
sleep 3
/soft/spark/bin/spark-submit \
--class huacloud.convertLibSVM.LibsvmffToLimsvm \
--master yarn-client \
--executor-cores 3 \
--num-executors 22 \
--driver-memory 20G \
--executor-memory 20G \
--jars /soft/spark/lib/hadoop-lzo-0.4.20-SNAPSHOT.jar \
-v /export/home/hcfruser/convertLibSVM-0.0.1-SNAPSHOT-jar-with-dependencies.jar \
"{'data_type':'"$data_type"','libsvmff_file':'"$result_path$table_name".libsvmff','label_table':'"$label_table"','result_path':'"$result_path"','client_no_colume':'user_id','label_colume':'label'}"
echo "sleep 3 seconds..."
sleep 3
done


## predict
modelPath=/export/grid/01/model_1222/
echo "get libsvm files..."
rm -rf ./${fileName}
mkdir ./${fileName}
hdfs dfs -get /hyzs/output/pin_label/*_test_${fileName} ./${fileName}

for((i=1;i<=7;i++))
do
resultPre=~/${fileName}/${i}_${fileName}
python /export/grid/01/js_run/predict.py  \
--path_test ~/${fileName}/${i}_test_${fileName}/${table_name}.libsvm   \
--path_pred ${resultPre}.pred \
--path_model /export/grid/01/model_1222/${i}_result/pin_${i}.model
rm -f ${resultPre}.result
python /export/grid/01/concat_index.py \
 ~/${fileName}/${i}_test_${fileName}/${table_name}.index \
  ${resultPre}.pred \
  ${resultPre}.result
echo "finished jobs, save pred to ${resultPre}.result"
done