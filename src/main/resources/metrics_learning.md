## 常用建模评价指标(Evaluation metrics)

* True Positive (TP) - 标签为正，预测为正
* True Negative (TN) - 标签为负，预测为负
* False Positive (FP) - 标签为负，预测为正
* False Negative (FN) - 标签为正，预测为负

### 混淆矩阵（Confusion Matrix）
![](https://note.youdao.com/yws/api/personal/file/7456F54FE899436D863546AAF7A20F77?method=download&shareKey=a823568a6551ae56eb90cedaf2c594a9)

### KS TEST
- 基于累计分布函数，用于检验数据是否符合某个分布或两个分布是否相同；
- 可以用来测量模型区分正例和负例的能力，即正例分布和负例分布的分离程度的度量；
- 下表显示了正负样本在不同区间上的统计数和累计个数：
![](https://note.youdao.com/yws/api/personal/file/1C8823E28461422B8ACB38FD8ADAEFC7?method=download&shareKey=6bf418d12724853f1c36c9fd099a534e "ks chart")
- 下图显示了正负样本的累计分布随区间阈值而变化的趋势：
![](https://note.youdao.com/yws/api/personal/file/324FAED6FCE84E9788B3C575056E7293?method=download&shareKey=97ea70dfce5e51be408470a935505275 "ks chart")
- 可发现，在第7个区间上，两个累计分布的间隔达到最大为（94%-12%=82%），即ks-test值为0.82

### ROC: Receiver operating characteristic（接收机操作特性）

![](https://note.youdao.com/yws/api/personal/file/756F1B92B64B4304AAEB6260D42EFB19?method=download&shareKey=2a0f9d84961d46e7978e4d7d4614079a "roc distribution")

- 可以用两个函数来分别表示样本空间中正例和负例的在模型预测分数上的分布情况；
- 设置一个临界值来划分模型预测的正例和负例的区间；
- 当临界值不断变化时，以（FPR，TPR）为点作出的曲线，即是roc曲线；

![](https://note.youdao.com/yws/api/personal/file/68406968B7014832A9A9D92366A5AAF8?method=download&shareKey=7dda95075b0c5ee38409defdcb15f440)

- 当两个分布分离的越开时，说明模型对正、负例区分的较好；
- 当两个分布重合的越多时，说明模型对正、负例区分能力较差；

### AUC: Area Under ROC Curve

![](https://note.youdao.com/yws/api/personal/file/409EEFE4D636423B9022ED9F60488D18?method=download&shareKey=6b22beb44139d97e26106d17648efa1a "roc")

- 当roc曲线下面积auc值为1时，意味着分类器能够完美的区分正例和负例，是一个完美分类器；
- roc值为0.5时，意味着分类器无法区分正例和负例，是完全随机的分类器；
- 正常的roc值在0.5和1之间，在0.8以上时即为好的分类器。

![](https://note.youdao.com/yws/api/personal/file/5A3A29DD71D04CB4B901B8F093A61652?method=download&shareKey=153e44ac0d28f6eb2a19c0adf22374cd "roc")

### DCG: discounted cumulative gain

### NDCG: Normalized DCG 

> https://en.wikipedia.org/wiki/Mean_absolute_error
> https://en.wikipedia.org/wiki/Root-mean-square_deviation
> https://en.wikipedia.org/wiki/Kolmogorov%E2%80%93Smirnov_test
> https://en.wikipedia.org/wiki/Discounted_cumulative_gain
> http://spark.apache.org/docs/2.2.1/mllib-evaluation-metrics.html
> https://en.wikipedia.org/wiki/Receiver_operating_characteristic
> http://www.saedsayad.com/flash/RocGainKS.html

### AUC，KS，RMSE对比试验

- test数据集为某次建模得到的真实预测结果集，其中正例2千，负例10万；
- good，normal，bad为使用不同高斯分布生成的随机数结果集；
- simu test为使用高斯分布模拟的test集的结果集；
- 模拟结果集与真实结果集，两者同时具有接近的AUC，KS，RMSE值，使用高斯分布能够在一定程度模拟具体模型的真实数据；

#### 模拟结果集

|dataset|source|AUC|KS|RMSE|
|-------|------|---|--|----|
|test|真实结果集|0.7840|0.5538|0.3137|
|simu test|Neg ~ N(0.15,0.1), Pos ~ N(0.9,1.0)|0.7857|0.5456|0.3202|

#### 优秀分类器结果集

|dataset|source|AUC|KS|RMSE|
|-------|------|---|--|----|
|good_1|Neg ~ N(0.3,0.1), Pos ~ N(0.8,0.1)|0.8621|0.5535|0.4218|
|good_2|Neg ~ N(0.3,0.01), Pos ~ N(0.8,0.01)|0.9997|0.9867|0.3153|
|good_3|Neg ~ N(0.4,0.01), Pos ~ N(0.9,0.01)|0.9998|0.9882|0.4092|

#### 一般分类器结果集

|dataset|source|AUC|KS|RMSE|
|-------|------|---|--|----|
|normal_1|Neg ~ N(0.1,0.3), Pos ~ N(0.9,0.5)|0.7960|0.4719|0.4173|
|normal_2|Neg ~ N(0.2,0.5), Pos ~ N(0.9,0.2)|0.7899|0.4698|0.5148|
|normal_3|Neg ~ N(0.3,0.5), Pos ~ N(0.8,0.5)|0.6912|0.3106|0.5567|

#### 较差分类器结果集

|dataset|source|AUC|KS|RMSE|
|-------|------|---|--|----|
|bad_1|Neg ~ N(0.4,1.0), Pos ~ N(0.7,1.0)|0.5831|0.1454|0.6303|
|bad_2|Neg ~ N(0.4,1.0), Pos ~ N(0.6,1.0)|0.5544|0.0993|0.6295|


- 以AUC值定义优秀、一般、较差三个分类器级别；
- good_2,good_3的数据显示，同样的分类器也可能具有波动大的RMSE值；
- good_1,normal_1的数据显示，较优的分类器也可能具有较大的RMSE值；
- RMSE值不适合用于评价分类器性能；
- 模型劣化时，KS值显示出比AUC更大的波动；
- KS会随AUC正相关变化，即KS也能代替AUC来评价模型性能；





### 计算公式

|metrics|definition|description|
|-------|----------|-----------|
|Precision (Positive Predictive Value)|\\(PPV=\frac{TP}{TP + FP}\\)|准确率|
|Recall (True Positive Rate)|\\(TPR=\frac{TP}{P}=\frac{TP}{TP + FN}\\)|召回率|
|FPR|\\(FPR = \frac{FP}{FP+TN}\\)||
|F-measure|\\(F(\beta) = \left(1 + \beta^2\right) \cdot \left(\frac{PPV \cdot TPR}{\beta^2 \cdot PPV + TPR}\right) = \frac{1}{\frac{1}{1+\beta^2}\frac{1}{\text{PPV}}+\frac{\beta^2}{1+\beta^2}\frac{1}{\text{TPR}}}\\) | \\(\beta\\)代表模型分类的偏好，当\\(\beta\\)小于1时，Precision更重要；<br/>当\\(\beta\\)大于1时，Recall更重要；<br/>当\\(\beta = 1\\)时，指标退化为F1。<br/>F1分数代表了模型对准确率和召回率的同时兼顾能力，最优为1，最差是0.|
|Receiver Operating Characteristic (ROC)|\\(FPR(T)=\int^\infty_{T} P_0(T)\,dT\\) <br/> \\(TPR(T)=\int^\infty_{T} P_1(T)\,dT\\)||
|Area Under ROC Curve|\\(AUROC=\int^1_{0} \frac{TP}{P} d\left(\frac{FP}{N}\right)\\)||
|Area Under Precision-Recall Curve	|\\(AUPRC=\int^1_{0} \frac{TP}{TP+FP} d\left(\frac{TP}{P}\right)\\)||
|MAE(mean absolute error)|\\(MAE = \frac{1}{n}\sum_{i=0}^n\mid y_i - \hat{y_i} \mid \\)|负向指标，值越小越好，取值范围0到无穷大；<br/>对比RMSE，易解释，易理解，易计算|
|RMSE(root mean square error)|\\(RMSE = \sqrt{\frac{1}{n}\sum_{i=0}^n {\({y_i} - \hat{y_i}\)}^2 }\\)|又作RMSD(root mean square deviation)，负向指标，值越小越好，取值范围0到无穷大；<br/>能更好的限制误差的量级，有效识别大误差|
|DCG|\\( DCG = rel_1+\sum_{i=2}^p \frac{rel_i}{\log_2 i} \\)|信息检索中，用来测量搜索引擎（排序系统，推荐系统）检索质量的评价指标，权重高的项排在前面的DCG值越大，越往后DCG值越小|
|NDCG|\\( NDCG = \frac{DCG}{iDCG}\\)|当权重以单调递减方式排序后，DCG可取到最大值，为iDCG（ideal DCG） |

<script type="text/javascript" src="http://cdn.mathjax.org/mathjax/latest/MathJax.js?config=default"></script>