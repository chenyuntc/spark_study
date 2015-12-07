#coding:utf8

from random import random
from pyspark import SparkContext
sc=SparkContext('local[*]','estimating pi')

MAX_NUM=1000000

def sample(x):
    return 1 if random()**2+random()**2<1 else 0

if __name__=='__main__':
    num=sc.parallelize(xrange(0,MAX_NUM)).map(sample).reduce(lambda x,y:x+y)
    print 'Pi is about to be  %s' %(float(num*4)/MAX_NUM,)
