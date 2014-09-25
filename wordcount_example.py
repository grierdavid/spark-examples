from pyspark import SparkContext

sc = SparkContext(appName="PythonWordcount")
lines = sc.textFile('hdfs://NAMENODE-1:8020/user/lavaqe/gutenberg')
lines_nonempty = lines.filter( lambda x: len(x) > 0 )
lines_nonempty.count()
words = lines_nonempty.flatMap(lambda x: x.split())
wordcounts = words.map(lambda x: (x, 1)).reduceByKey(lambda x,y:x+y).map(lambda x:(x[1],x[0])).sortByKey(False)
print wordcounts.take(10)
