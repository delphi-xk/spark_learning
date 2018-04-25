## 常用建模评价指标(Evaluation metrics)

* True Positive (TP) - label is positive and prediction is also positive
* True Negative (TN) - label is negative and prediction is also negative
* False Positive (FP) - label is negative but prediction is positive
* False Negative (FN) - label is positive but prediction is negative


|metrics|definition|
|-------|----------|
|Precision (Positive Predictive Value)|\\(PPV=\frac{TP}{TP + FP}\\)|
|Recall (True Positive Rate)|\\(TPR=\frac{TP}{P}=\frac{TP}{TP + FN}\\)|
|F-measure|\\(F(\beta) = \left(1 + \beta^2\right) \cdot \left(\frac{PPV \cdot TPR}{\beta^2 \cdot PPV + TPR}\right)\\)|
|Receiver Operating Characteristic (ROC)|\\(FPR(T)=\int^\infty_{T} P_0(T)\,dT\\) <br/> \\(TPR(T)=\int^\infty_{T} P_1(T)\,dT\\)|
|Area Under ROC Curve|\\(AUROC=\int^1_{0} \frac{TP}{P} d\left(\frac{FP}{N}\right)\\)|
|Area Under Precision-Recall Curve	|\\(AUPRC=\int^1_{0} \frac{TP}{TP+FP} d\left(\frac{TP}{P}\right)\\)|

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