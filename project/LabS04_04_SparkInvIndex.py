from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
import os, re

#spark = SparkContext.getOrCreate(SparkConf())
sc = SparkContext.getOrCreate(SparkConf())
#spark = SparkSession.builder.master('spark://p930026096:7077').appName('MySparkInvIndex').getOrCreate()
#sc = spark.sparkContext

inverted_index = sc.wholeTextFiles('hdfs://10.20.4.50:9000/user/hduser/input/books_2528_1000M/*.txt').flatMap(lambda x:[((os.path.basename(x[0]).split(".")[0] ,i) ,1) for i in re.split('\\W', x[1])]).reduceByKey(lambda a, b: a + b).map(lambda x: (x[0][1],(x[0][0],x[1])))

output = inverted_index.collect()

sc.stop()

inverted_index_dict= {}
for i in range(len(output)):
    if output[i][0] in inverted_index_dict.keys():
        inverted_index_dict[output[i][0]].append(output[i][1][0])
    else:
        inverted_index_dict[output[i][0]]=list(output[i][1][0])
#print(inverted_index_dict)
file = open('inverted_index.txt','w')
for k,v in inverted_index_dict.items():
    if k!= '':
        file.write(str(k)+' '+str(v)+'\n')
file.close()

