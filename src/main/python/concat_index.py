# -*- coding: UTF-8 -*-

import sys
indexPath = sys.argv[1]
predPath = sys.argv[2]
resPath = sys.argv[3]
index = open(indexPath)
pred = open(predPath)
res = open(resPath, 'w')
indexLines = index.readlines()
predLines = pred.readlines()
if len(indexLines) == len(predLines):
    print("file lines equals")
    for i in range(len(indexLines)):
        res.write(indexLines[i].strip()+"\t"+predLines[i])
    res.close()
else:
    print("file rows not equal!")