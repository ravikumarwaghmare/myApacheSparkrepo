
from pyspark import SparkContext, SparkConf
from pyspark.sql import Row, SparkSession

conf = SparkConf().setAppName("RDD to SQL").setMaster("local")
sc = SparkContext(conf=conf)

spark = SparkSession.builder.appName("RDD to SQL").getOrCreate()


###Load txt to convert each line to row

lines = sc.textFile("/apps/spark-2.3.0-bin-hadoop2.7/rkw_spark_problems/people.txt")

parts = lines.map(lambda l: l.split(","))

    
people = parts.map(lambda p: Row(name=p[0], age=int(p[1])))


###Infer schema and register to DF


schemaPeople = spark.createDataFrame(people)
schemaPeople.createOrReplaceTempView("people")


###SQL can be run over created DF

Aged = spark.sql("select name from people where age > 33")

###Reslt of SQL queries are DF objects

###rdd returns the content as an :class: pyspark.RDD.

Agedpeople = Aged.rdd.map(lambda p: "Name:" +p.name).collect()


for name in Agedpeople:
    print(name)


