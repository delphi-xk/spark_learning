#!/bin/bash

cp /export/grid/01/model_pin_1/result/pin_1.feature ~/
cp /export/grid/01/model_pin_2/result/pin_2.feature ~/
cp /export/grid/01/model_pin_3/result/pin_3.feature ~/
cp /export/grid/01/model_pin_4/result/pin_4.feature ~/
cp /export/grid/01/model_pin_5/result/pin_5.feature ~/
cp /export/grid/01/model_pin_6/result/pin_6.feature ~/
cp /export/grid/01/model_pin_7/result/pin_7.feature ~/
tail pin_*.feature >> all-features.txt