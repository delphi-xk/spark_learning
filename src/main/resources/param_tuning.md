## gbt

|object|iteration|learning_rate|validation_toll|depth|valid_result|test_result|
|---|---|---|---|---|---|---|
|regression|100|0.001|0.000001|6|3.82|3.906|


## xgboost

|object|booster|eval_metric|max_depth|eta|gamma|alpha|subsample|num_round|**train_result**|**valid_result**|**test_result**|
|---|---|---|---|---|---|----|---|----|---|---|---|
|reg:linear|gbtree|rmse|4|0.01|0.5|0.007|0.7|800|3.99 ~ 3.81|3.740|3.884|
|reg:linear|gbtree|rmse|5|0.01|0.3|0.001|0.8|800|3.99 ~ 3.76|3.742|na|
|reg:linear|gbtree|rmse|5|0.01|0.5|na|0.5|800|3.99 ~ 3.76|3.74|3.885|
|reg:linear|gbtree|rmse|5|0.1|0.5|0.007|0.7|800|3.99 ~ 3.40|3.81|3.952|
|reg:linear|gbtree|rmse|5|0.1|0.5|0.01|0.5|400|3.97 ~ 3.57|3.789|na|
|reg:linear|gbtree|rmse|7|0.01|0.5|na|0.7|800|3.91 ~ 3.50|3.87|3.886|
|reg:linear|gbtree|rmse|6|0.01|0.3|na|0.8|800|3.99 ~ 3.69|3.745|na|
