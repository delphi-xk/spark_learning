#!/usr/bin/env bash


for((i=5;i<=5;i++))
do
data_type=train
table_name=hyzs.pin_data_train
result_path=/hyzs/output/pin_label/${i}_train/
#train不需指定obj文件，valid及test需指定obj文件
obj_file=
#train及valid需指定label表，test不需指定label
label_table=hyzs.pin_label_$i


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