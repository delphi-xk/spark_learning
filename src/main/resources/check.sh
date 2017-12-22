#!/usr/bin/env bash

for((i=1;i<=7;i++))
do
  for t in train valid test
  do
    echo "i: ${i}, t: ${t}"
    wc -l ./model_1222/${i}_${t}/hyzs.pin_data_${t}.libsvm
    wc -l ./model_1222/${i}_${t}/hyzs.pin_data_${t}.index
  done
done
