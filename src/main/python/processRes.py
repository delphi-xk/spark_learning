import pandas as pd
df = pd.read_csv("C:/Users/XIANGKUN/Desktop/jd-job/pin7result_2.txt",header=None)
res = df[[0,1,10,11,12,13,14,15,16]]
res.columns = ['phone','label','pred_1','pred_2','pred_3','pred_4','pred_5','pred_6','pred_7']
res.to_csv("C:/Users/XIANGKUN/Desktop/jd-job/pin7result_2.csv")