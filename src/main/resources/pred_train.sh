#!/usr/bin/env bash

for((i=1;i<=7;i++))
do
    echo "process ${i}th prediction..."
    python /export/grid/01/js_run/predict.py  \
    --path_test /export/grid/01/model_pin_${i}/libsvm/train.libsvm \
    --path_pred /export/grid/01/model_pin_${i}/result/train_pin_${i}.pred \
    --path_model /export/grid/01/model_pin_${i}/result/pin_${i}.model
done
