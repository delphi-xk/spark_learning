##1 从csv,txt导入Spark建表
- header文件以“,”分隔，data文件以“\t”分隔，列数需要相同
```shell
/soft/spark/bin/spark-submit  --master yarn-client \
--class com.hyzs.spark.sql.JDDataProcess \
--driver-memory 20G --num-executors 22 --executor-memory 24G --executor-cores 2 \
--jars /soft/spark/lib/hadoop-lzo-0.4.20-SNAPSHOT.jar \
--conf "spark.sql.shuffle.partitions=1000"
--conf "spark.driver.extraJavaOptions=-Dderby.system.home=${dbHome}" \
--conf "spark.processJob.dataPath=${dataPath}" \
--conf "spark.processJob.headerPath=${headerPath}" \
--conf "spark.processJob.fileNames=${fileNames}" \
/export/home/hcfruser/DataProcess.jar
```
- dbHome为数据仓库元数据存储的本机地址，迁移时需要拷贝整个目录，dataPath为数据文件路径，headerPath为表头文件路径(默认为HDFS路径),fileNames为若干文件名用“,”拼接的字符串，例
```shell
dbHome=/export/grid/01/database/
dataPath=/user/hyzs/output_20180109/data/
headerPath=/user/hyzs/output_20180109/header/
fileNames=dmt_upf_s_d_45,dmt_upf_s_d_42,dmt_upf_s_d_50……
```

##2 执行Spark任务，生成Libsvm文件

- 以下shell路径除特别说明，都为HDFS路径，可自定义
```shell
for task in m1 m2 m3
do
	for data_type in train valid test
	do
	result_path=/hyzs/output/point/${task}/${data_type}/
	table_name=hyzs.${task}_${data_type}
	obj_file=/hyzs/output/point/${task}/train/hyzs.${task}_train.obj
	label_table=${task}_label

	/soft/spark/bin/spark-submit \
	--class huacloud.convertLibSVM.ConvertLibSvmFF \
	--master yarn-client \
	--executor-cores 2 --num-executors 22 --driver-memory 20G --executor-memory 20G \
	--conf 'spark.driver.maxResultSize=2048' \
	--jars /soft/spark/lib/hadoop-lzo-0.4.20-SNAPSHOT.jar \
	-v /export/home/hcfruser/convertLibSVM.jar \
	"{'data_type':'"${data_type}"','table_name':'"${table_name}"',
	'result_path':'"${result_path}"','client_no':'user_id','obj_file':'"${obj_file}"'}"

	/soft/spark/bin/spark-submit \
	--class huacloud.convertLibSVM.LibsvmffToLimsvm \
	--master yarn-client \
	--executor-cores 2 --num-executors 22 --driver-memory 20G --executor-memory 20G \
	--jars /soft/spark/lib/hadoop-lzo-0.4.20-SNAPSHOT.jar \
	-v /export/home/hcfruser/convertLibSVM.jar \
	"{'data_type':'"${data_type}"','libsvmff_file':'"${result_path}${table_name}".libsvmff',
	'label_table':'"${label_table}"','result_path':'"${result_path}"',
	'client_no_colume':'user_id','label_colume':'label'}"
	done
done

```
- 结果会在hdfs://hyzs/output/point/下生成三个任务目录m1, m2, m3（暂定的三个任务），每个目录下会生成对应train，valid，test目录
- **如果不需要训练模型，则只有test过程**

```
data_type=test
for task in m1 m2 m3
do
    result_path=/hyzs/output/point/${task}/${data_type}/
    table_name=hyzs.${task}_${data_type}
    obj_file=/hyzs/output/point/${task}/train/hyzs.${task}_train.obj
    label_table=${task}_label

    /soft/spark/bin/spark-submit \
    --class huacloud.convertLibSVM.ConvertLibSvmFF \
    --master yarn-client \
    --executor-cores 2 --num-executors 22 --driver-memory 20G --executor-memory 20G \
    --conf 'spark.driver.maxResultSize=2048' \
    --jars /soft/spark/lib/hadoop-lzo-0.4.20-SNAPSHOT.jar \
    -v /export/home/hcfruser/convertLibSVM.jar \
    "{'data_type':'"${data_type}"','table_name':'"${table_name}"',
    'result_path':'"${result_path}"','client_no':'user_id','obj_file':'"${obj_file}"'}"

    /soft/spark/bin/spark-submit \
    --class huacloud.convertLibSVM.LibsvmffToLimsvm \
    --master yarn-client \
    --executor-cores 2 --num-executors 22 --driver-memory 20G --executor-memory 20G \
    --jars /soft/spark/lib/hadoop-lzo-0.4.20-SNAPSHOT.jar \
    -v /export/home/hcfruser/convertLibSVM.jar \
    "{'data_type':'"${data_type}"','libsvmff_file':'"${result_path}${table_name}".libsvmff',
    'label_table':'"${label_table}"','result_path':'"${result_path}"',
    'client_no_colume':'user_id','label_colume':'label'}"
done
```


##3 将HDFS上生成文件拖到本地**basePath**下
训练模型

- 以下路径为本地路径
- sample_path为训练libsvm文件
- validation_path为validation文件路径
```shell
hdfs dfs -get  /hyzs/output/point/* ${basePath}
for task in m1 m2 m3
do
	python /export/grid/01/js_run/train.py \
    --params '{"sample_path":"${basePath}/${task}/train/hyzs.${task}_train.libsvm",
    "sample_auc_path":"${basePath}/${task}/result/log.gbt-train-auc",
    "sample_name_path":"${basePath}/${task}/train/hyzs.${task}_train.name",
    "sample_index_path":"${basePath}/${task}/train/hyzs.${task}_train.index",
    "validation_path":"${basePath}/${task}/valid/hyzs.${task}_valid.libsvm",
    "validation_auc_path":"${basePath}/${task}/result/log.gbt-valid-auc",
    "features_path":"${basePath}/${task}/result/${task}.feature",
    "tree_path":"${basePath}/${task}/result/${task}.tree",
    "feature_explain_path":"${basePath}/${task}/result/${task}.features-epl",
    "model_store_path":"${basePath}/${task}/result/${task}.model",
    "objective":"reg:linear","eval_metric":"auc","bst:eta":"0.01",
    "subsample":"0.5","bst:max_depth":"5","gamma":"0.5"}'
done
```
**如果不需要训练模型，则只有test过程**，根据模型文件，输入需要预测的文件，得到预测结果

- path_test为预测的libsvm文件
- test_pred为预测的结果文件
- path_mode为预测的模型
```shell
for task in m1 m2 m3
do
    python /export/grid/01/js_run/predict.py  \
    --path_test ${basePath}/${task}/test/hyzs.${task}_test.libsvm  \
    --path_pred ${basePath}/${task}/result/hyzs.${task}_test.pred \
    --path_model ${basePath}/${task}/result/${task}.model
done
```

生成结果文件
```shell
for task in m1 m2 m3
do
python /export/grid/01/concat_index.py \
  ${basePath}/${task}/test/hyzs.${task}_test.index \
  ${basePath}/${task}/result/hyzs.${task}_test.pred \
  ${basePath}/${task}/result/hyzs.${task}_test.result
done
```

##4 建模任务名对应列名

建模任务名 | HBase列名
--- | --- 
consume | m1
value | m2
risk | m3
class1 | c1
class2 | c2
class3 | c3
class4 | c4
class5 | c5
class6 | c6
class7 | c7