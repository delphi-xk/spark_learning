#!/bin/sh

localBasePath=$1
hbasePath=$2

echo "localBasePath=${localBasePath}"

if [ ! -d ${localBasePath} ]; then
  mkdir ${localBasePath}
fi

hdfs dfs -get /user/hyzs/output/point/* ${localBasePath}
for task in m1 m2 m3
do
  if [ ! -d "${localBasePath}/${task}/result" ]; then
      mkdir ${localBasePath}/${task}/result
  fi

  mkdir ${localBasePath}/${task}/result
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
done

for task in m1 m2 m3
do
python /export/grid/03/qiuyujiang/hyzs/hyzs_shell/concat_index.py \
  ${localBasePath}/${task}/test/hyzs.${task}_test.index \
  ${localBasePath}/${task}/result/hyzs.${task}_test.pred \
  ${localBasePath}/${task}/result/hyzs.${task}_test.result

sed -i "s/$/&,${task}/g" ${localBasePath}/${task}/result/hyzs.${task}_test.result
hdfs dfs -put ${localBasePath}/${task}/result/hyzs.${task}_test.result ${hbasePath}
done

echo model training step 3 finished

