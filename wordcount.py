#coding:utf8
from pyspark import  SparkContext

sc=SparkContext('local[*]','cy')
text=sc.textFile('/home/cy/httpd.conf')
words=text.flatMap(lambda x: x.split()).map(lambda x: (x,1)).reduceByKey(lambda a,b:a+b)
words.saveAsTextFile('wc3')
sc.stop()
