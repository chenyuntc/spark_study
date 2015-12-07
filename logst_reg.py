#coding:utf8
from numpy import exp
from random import random
ITERATE_NUM=100
import numpy as np
from pyspark import SparkContext
sc=SparkContext('local[*]','logist regression')
'''
generate the test file which is like
0.1,0.1,0.2,0.3....,0
'''

f=open('a.txt','w')
for ii in range(1000):
    i2=1 if random()>0.7 else 0
    if i2==1:
        q=[random()*0.75+0.2 for i in range(10)]
        for i in q:
            f.write(str(i)+',')
        f.write(str(1)+'\r\n')
    else:
        q=[random()*0.25+0.2 for i in range(10)]
        for i in q:f.write(str(i)+',')
        f.write(str(0)+'\r\n')
f.close()



t=sc.textFile('/tmp/内存/a.txt')
t2=t.map(lambda x:( map(float,x.split(',')[:-1]), int(x.split(',')[-1])) ).cache()


# calculate
w=np.random.ranf(size=(1,10))
for i in range(ITERATE_NUM):
    grad=t2.map( lambda p: (1 / (1 + exp(-p[1]*(w.dot(p[0])))) - 1) *np.array( p[0]) *np.array( p[1])).reduce(lambda a,b:a+b);
    w -= grad

# check the result 
#print 0 means approximate right
t2s=t2.take(1000)
for ii in t2s:
    print 1-ii[1] if w.dot(ii[0])>40 else 0-ii[1]

 

