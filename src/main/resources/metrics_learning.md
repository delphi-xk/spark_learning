## 常用建模评价指标(Evaluation metrics)

* True Positive (TP) - 标签为正，预测为正
* True Negative (TN) - 标签为负，预测为负
* False Positive (FP) - 标签为负，预测为正
* False Negative (FN) - 标签为正，预测为负


|metrics|definition|description|
|-------|----------|-----------|
|Precision (Positive Predictive Value)|\\(PPV=\frac{TP}{TP + FP}\\)|准确率|
|Recall (True Positive Rate)|\\(TPR=\frac{TP}{P}=\frac{TP}{TP + FN}\\)|召回率|
|FPR|\\(FPR = \frac{FP}{FP+TN}\\)||
|F-measure|\\(F(\beta) = \left(1 + \beta^2\right) \cdot \left(\frac{PPV \cdot TPR}{\beta^2 \cdot PPV + TPR}\right) = \frac{1}{\frac{1}{1+\beta^2}\frac{1}{\text{PPV}}+\frac{\beta^2}{1+\beta^2}\frac{1}{\text{TPR}}}\\) | \\(\beta\\)代表模型分类的偏好，当\\(\beta\\)小于1时，Precision更重要；<br/>当\\(\beta\\)大于1时，Recall更重要；<br/>当\\(\beta = 1\\)时，指标退化为F1。|
|Receiver Operating Characteristic (ROC)|\\(FPR(T)=\int^\infty_{T} P_0(T)\,dT\\) <br/> \\(TPR(T)=\int^\infty_{T} P_1(T)\,dT\\)||
|Area Under ROC Curve|\\(AUROC=\int^1_{0} \frac{TP}{P} d\left(\frac{FP}{N}\right)\\)||
|Area Under Precision-Recall Curve	|\\(AUPRC=\int^1_{0} \frac{TP}{TP+FP} d\left(\frac{TP}{P}\right)\\)||
|MAE|\\(MAE = \frac{1}{n}\sum_{i=0}^n\mid y_i - \hat{y_i} \mid \\)|负向指标，值越小越好，取值范围0到无穷大；<br/>对比RMSE，易解释，易理解，易计算|
|RMSE|\\(RMSE = \sqrt{\frac{1}{n}\sum_{i=0}^n {\({y_i} - \hat{y_i}\)}^2 }\\)|又作RMSD(root mean square deviation)，负向指标，值越小越好，取值范围0到无穷大；<br/>能更好的限制误差的量级，有效识别大误差|

### MAE: mean absolute error

### RMSE: root mean square error

### KS TEST

### DCG: discounted cumulative gain

### NDCG: Normalized DCG 

### AUC: Area Under ROC Curve(Receiver operating characteristic)

### AUPRC: Area Under Precision-Recall Curve


> https://en.wikipedia.org/wiki/Mean_absolute_error
> https://en.wikipedia.org/wiki/Root-mean-square_deviation
> https://en.wikipedia.org/wiki/Kolmogorov%E2%80%93Smirnov_test
> https://en.wikipedia.org/wiki/Discounted_cumulative_gain
> http://spark.apache.org/docs/2.2.1/mllib-evaluation-metrics.html
> https://en.wikipedia.org/wiki/Receiver_operating_characteristic

<script type="text/javascript" src="http://cdn.mathjax.org/mathjax/latest/MathJax.js?config=default"></script>