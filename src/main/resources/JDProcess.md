## Step1. 从csv,txt导入Spark建表
- header文件以“,”分隔，data文件以“\t”分隔，列数需要相同
- dbHome为数据仓库元数据存储的本机地址，迁移时需要拷贝整个目录，dataPath为数据文件路径，headerPath为表头文件路径(HDFS路径),fileNames为若干文件名用“,”拼接的字符串，例
- joinType为join操作类型，可取值left,right,full
```shell
dbHome=/export/grid/03/qiuyujiang/hyzs/dbHome/
dataPath=/user/hyzs/data/20180121/
headerPath=/user/hyzs/header/
fileNames=dmr_rec_a_ords_hyzs_s_d,dmr_rec_a_feas_hyzs_s_d
joinType=right
resTable=all_data

/soft/spark/bin/spark-submit --master yarn-client \
--class com.hyzs.spark.sql.JDDataProcess \
--num-executors 19 --driver-memory 10G --executor-memory 22G --executor-cores 3 \
--jars /soft/spark/lib/hadoop-lzo-0.4.20-SNAPSHOT.jar \
--conf "spark.sql.shuffle.partitions=1500" \
--conf "spark.driver.extraJavaOptions=-Dderby.system.home=${dbHome}" \
--conf "spark.processJob.dataPath=${dataPath}" \
--conf "spark.processJob.headerPath=${headerPath}" \
--conf "spark.processJob.fileNames=${fileNames}" \
--conf "spark.processJob.joinType=${joinType}" \
--conf "spark.processJob.resultTable=${resTable}" \ 
/export/grid/03/qiuyujiang/hyzs/libs/DataProcess.jar [args0] [args1] [args2]
```
-------
|参数名 | 取值 | 含义
|--- | --- | --- 
|**args0** | import_business | 导入订单表（有额外流水表处理，默认为第一张表）
| | import_info | 导入标签信息表（无其他处理，默认第二张表开始的之后所有表）
| | import_all | 同时执行前两步操作
| | skip_import | 不执行导入操作
|**args1** | join | 执行合表操作
| | skip_join | 跳过合表操作
|**args2** | train | 生成train数据集
| | test | 生成test数据集

## Step2. 执行Spark任务，生成Libsvm文件

### 2.1**训练模型**
- 会生成train, valid, test三个数据集
- 以下路径为HDFS路径，可自定义
- 结果会在 hdfs://${basePath}/output/point/ 下生成三个任务目录m1, m2, m3（暂定的三个任务），每个目录下会生成对应train，valid，test目录

```shell
dbHome=/export/grid/03/qiuyujiang/hyzs/dbHome/
basePath=/user/hyzs

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
  --num-executors 19 --driver-memory 10G --executor-memory 22G \
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
  --num-executors 19 --driver-memory 10G --executor-memory 22G \
  --
  conf "spark.driver.extraJavaOptions=-Dderby.system.home=${dbHome}" \
  --jars /soft/spark/lib/hadoop-lzo-0.4.20-SNAPSHOT.jar \
  -v /export/grid/03/qiuyujiang/hyzs/libs/convertLibSVM.jar \
  "{'data_type':'"${data_type}"','libsvmff_file':'"${result_path}${table_name}".libsvmff',
  'label_table':'"${label_table}"','result_path':'"${result_path}"',
  'client_no_colume':'user_id_md5','label_colume':'label'}"

  sleep 5s

  done
done

```

### 2.2**预测数据**
- 只需生成test数据

```shell
dbHome=/export/grid/03/qiuyujiang/hyzs/dbHome/
basePath=/user/hyzs
data_type=test

for task in m1 m2 m3
do
    result_path=${basePath}/output/point/${task}/${data_type}/
    table_name=hyzs.${task}_${data_type}
    obj_file=${basePath}/output/point/${task}/train/hyzs.${task}_train.obj
    label_table=hyzs.${task}_label

    /soft/spark/bin/spark-submit \
    --class huacloud.convertLibSVM.ConvertLibSvmFF \
    --master yarn-client \
    --num-executors 19 --driver-memory 10G --executor-memory 22G \
    --conf 'spark.driver.maxResultSize=2048' \
    --jars /soft/spark/lib/hadoop-lzo-0.4.20-SNAPSHOT.jar \
    -v /export/home/hcfruser/convertLibSVM.jar \
    "{'data_type':'"${data_type}"','table_name':'"${table_name}"',
    'result_path':'"${result_path}"','client_no':'user_id_md5','obj_file':'"${obj_file}"'}"

    sleep 5s

    /soft/spark/bin/spark-submit \
    --class huacloud.convertLibSVM.LibsvmffToLimsvm \
    --master yarn-client \
    --num-executors 19 --driver-memory 10G --executor-memory 22G \
    --jars /soft/spark/lib/hadoop-lzo-0.4.20-SNAPSHOT.jar \
    -v /export/home/hcfruser/convertLibSVM.jar \
    "{'data_type':'"${data_type}"','libsvmff_file':'"${result_path}${table_name}".libsvmff',
    'label_table':'"${label_table}"','result_path':'"${result_path}"',
    'client_no_colume':'user_id_md5','label_colume':'label'}"

    sleep 5s
done
```


## 3 建模预测，并上传结果

### 3.1 **训练模型**

- 将HDFS上生成文件拖到本地**localBasePath**下
- 将模型预测结果上传**hbasePath**
- sample_path为训练libsvm文件
- validation_path为validation文件路径

```shell
localBasePath=$1
hbasePath=$2

echo "localBasePath=${localBasePath}"
echo "hbasePath=${hbasePath}"

if [ ! -d ${localBasePath} ]; then
  mkdir ${localBasePath}
fi

hdfs dfs -get /user/hyzs/output/point/* ${localBasePath}
for task in m1 m2 m3
do
  if [ ! -d "${localBasePath}/${task}/result" ]; then
      mkdir ${localBasePath}/${task}/result
  fi

  python /export/grid/03/qiuyujiang/hyzs/hyzs_shell/js_run/train.py \
    --params '{"sample_path":"'${localBasePath}'/'${task}'/train/hyzs.'${task}'_train.libsvm",
    "sample_auc_path":"'${localBasePath}'/'${task}'/result/log.gbt-train-auc",
    "sample_name_path":"'${localBasePath}'/'${task}'/train/hyzs.'${task}'_train.name",
    "sample_index_path":"'${localBasePath}'/'${task}'/train/hyzs.'${task}'_train.index",
    "validation_path":"'${localBasePath}'/'${task}'/valid/hyzs.'${task}'_valid.libsvm",
    "validation_auc_path":"'${localBasePath}'/'${task}'/result/log.gbt-valid-auc",
    "features_path":"'${localBasePath}'/'${task}'/result/'${task}'.feature",
    "tree_path":"'${localBasePath}'/'${task}'/result/'${task}'.tree",
    "feature_explain_path":"'${localBasePath}'/'${task}'/result/'${task}'.features-epl",
    "model_store_path":"'${localBasePath}'/'${task}'/result/'${task}'.model",
    "objective":"reg:linear","eval_metric":"auc","bst:eta":"0.01","subsample":"0.5","bst:max_depth":"5","gamma":"0.5"}'

  python /export/grid/03/qiuyujiang/hyzs/hyzs_shell/js_run/predict.py \
    --path_test ${localBasePath}/${task}/test/hyzs.${task}_test.libsvm \
    --path_pred ${localBasePath}/${task}/result/hyzs.${task}_test.pred \
    --path_model ${localBasePath}/${task}/result/${task}.model

  python /export/grid/03/qiuyujiang/hyzs/hyzs_shell/concat_index.py \
    ${localBasePath}/${task}/test/hyzs.${task}_test.index \
    ${localBasePath}/${task}/result/hyzs.${task}_test.pred \
    ${localBasePath}/${task}/result/hyzs.${task}_test.result

  sed -i "s/$/&,${task}/g" ${localBasePath}/${task}/result/hyzs.${task}_test.result
  hdfs dfs -put ${localBasePath}/${task}/result/hyzs.${task}_test.result ${hbasePath}

done

echo model training step 3 finished
```

### 3.2 **预测数据**
- 只需生成test数据，根据模型文件，输入需要预测的文件，得到预测结果

```shell

localBasePath=$1
hbasePath=$2

echo "localBasePath=${localBasePath}"
echo "hbasePath=${hbasePath}"

if [ ! -d ${localBasePath} ]; then
  mkdir ${localBasePath}
fi

hdfs dfs -get /user/hyzs/output/point/* ${localBasePath}

for task in m1 m2 m3
do
  if [ ! -d "${localBasePath}/${task}/result" ]; then
    mkdir ${localBasePath}/${task}/result
  fi

  python /export/grid/03/qiuyujiang/hyzs/hyzs_shell/js_run/predict.py \
    --path_test ${localBasePath}/${task}/test/hyzs.${task}_test.libsvm \
    --path_pred ${localBasePath}/${task}/result/hyzs.${task}_test.pred \
    --path_model ${localBasePath}/${task}/result/${task}.model

  python /export/grid/03/qiuyujiang/hyzs/hyzs_shell/concat_index.py \
    ${localBasePath}/${task}/test/hyzs.${task}_test.index \
    ${localBasePath}/${task}/result/hyzs.${task}_test.pred \
    ${localBasePath}/${task}/result/hyzs.${task}_test.result

  sed -i "s/$/&,${task}/g" ${localBasePath}/${task}/result/hyzs.${task}_test.result
  hdfs dfs -put ${localBasePath}/${task}/result/hyzs.${task}_test.result ${hbasePath}

done

echo model training step 3 finished

```


## 4 结果说明

- 结果文件在对应模型的result目录下的.result文件
- 结果文件格式为: id, prediction, hbaseCol

-------
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

## 5 代码调试
### 转换libsvm时速度慢
- stringindexer transform效率低下
- pipeline长stage效率低下，使用自定义udf转换数据