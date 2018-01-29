#!/bin/sh

dbHome=$1
basePath=$2
localBasePath=$3

echo "dbHome=${dbHome}"
echo "basePath=${basePath}"
echo "localBasePath=${localBasePath}"

for task in m1 m2 m3
do
  for data_type in train valid test
  do
  result_path=${basePath}/output/point/${task}/${data_type}/
  table_name=hyzs.${task}_${data_type}
  obj_file=${basePath}/output/point/${task}/train/hyzs.${task}_train.obj
  label_table=hyzs.${task}_label

  /soft/spark/bin/spark-submit \
  --class huacloud.convertLibSVM.ConvertLibSvmFF \
  --master yarn-client \
  --executor-cores 2 --num-executors 15 --driver-memory 20G --executor-memory 20G \
  --conf 'spark.driver.maxResultSize=2048' \
  --conf "spark.driver.extraJavaOptions=-Dderby.system.home=${dbHome}" \
  --jars /soft/spark/lib/hadoop-lzo-0.4.20-SNAPSHOT.jar \
  -v /export/grid/03/qiuyujiang/hyzs/libs/convertLibSVM.jar \
  "{'data_type':'"${data_type}"','table_name':'"${table_name}"',
  'result_path':'"${result_path}"','client_no':'user_id_md5','obj_file':'"${obj_file}"'}"

  sleep 5s

  /soft/spark/bin/spark-submit \
  --class huacloud.convertLibSVM.LibsvmffToLimsvm \
  --master yarn-client \
  --executor-cores 2 --num-executors 15 --driver-memory 20G --executor-memory 20G \
  --conf "spark.driver.extraJavaOptions=-Dderby.system.home=${dbHome}" \
  --jars /soft/spark/lib/hadoop-lzo-0.4.20-SNAPSHOT.jar \
  -v /export/grid/03/qiuyujiang/hyzs/libs/convertLibSVM.jar \
  "{'data_type':'"${data_type}"','libsvmff_file':'"${result_path}${table_name}".libsvmff',
  'label_table':'"${label_table}"','result_path':'"${result_path}"','client_no_colume':'user_id_md5','label_colume':'label'}"

  sleep 5s

  done
done


echo model training step 2 finished
