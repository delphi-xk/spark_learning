#!/usr/bin/env bash

model=model_1222
file_path=/export/grid/01/${model}

for((i=1;i<=7;i++))
do
    if [ ! -d "${file_path}/${i}_result" ]
    then
        mkdir ${file_path}/${i}_result
    fi
    python /export/grid/01/js_run/train.py \
    --params '{"sample_path":"/export/grid/01/'${model}/${i}'_train/hyzs.pin_data_train.libsvm",
    "sample_auc_path":"/export/grid/01/'${model}/${i}'_result/log.gbt-train-auc",
    "sample_name_path":"/export/grid/01/'${model}/${i}'_train/hyzs.pin_data_train.name",
    "sample_index_path":"/export/grid/01/'${model}/${i}'_train/train.index",
    "validation_path":"/export/grid/01/'${model}/${i}'_valid/hyzs.pin_data_valid.libsvm",
    "validation_auc_path":"/export/grid/01/'${model}/${i}'_result/log.gbt-valid-auc",
    "features_path":"/export/grid/01/'${model}/${i}'_result/pin_'${i}'.feature",
    "tree_path":"/export/grid/01/'${model}/${i}'_result/pin_'${i}'.tree",
    "feature_explain_path":"/export/grid/01/'${model}/${i}'_result/pin_'${i}'.features-epl",
    "model_store_path":"/export/grid/01/'${model}/${i}'_result/pin_'${i}'.model",
    "objective":"reg:linear","eval_metric":"auc","bst:eta":"0.01","subsample":"0.5","bst:max_depth":"5","gamma":"0.5"}'

    python /export/grid/01/js_run/predict.py  \
    --path_test /export/grid/01/${model}/${i}_test/hyzs.pin_data_test.libsvm  \
    --path_pred /export/grid/01/${model}/${i}_result/pin_${i}.pred \
    --path_model /export/grid/01/${model}/${i}_result/pin_${i}.model
done