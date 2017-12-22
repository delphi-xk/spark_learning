# -*- coding: UTF-8 -*-
for x in range(1,8):
    taskName = "pin_" + str(x)
    index = open("model_"+taskName+"/test/hyzs.pin_data_test.index")
    pred = open("model_"+taskName+"/result/"+taskName+".pred")
    res = open(taskName+".result", 'w')
    indexLines = index.readlines()
    predLines = pred.readlines()
    if len(indexLines) == len(predLines):
        print("file lines equals")
        for i in range(len(indexLines)):
            res.write(indexLines[i].rstrip("\n")+","+predLines[i])
        res.close()
    else:
        print("file lines not equal!")