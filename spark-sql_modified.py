from pyspark.sql import SparkSession
from pyspark.sql import Row

import collections

# Create a SparkSession (Note, the config section is only for Windows!)
#spark = SparkSession.builder.config("spark.sql.warehouse.dir", "file:///C:/temp").appName("SparkSQL").getOrCreate()

##Below modified for Linux

#spark = SparkSession.builder.master("local").appName("SparkSQL").getOrCreate()
spark = SparkSession.builder.master("local").appName("SparkSQL").getOrCreate()


def mapper(line):
    fields = line.split(',')
    return Row(ID=int(fields[0]), name=str(fields[1].encode("utf-8")), age=int(fields[2]), numFriends=int(fields[3]))

lines = spark.sparkContext.textFile("file:////opt/spark/SparkCourse/fakefriends.csv")
people = lines.map(mapper)

# Infer the schema, and register the DataFrame as a table.
schemaPeople = spark.createDataFrame(people).cache()
schemaPeople.createOrReplaceTempView("people")

# SQL can be run over DataFrames that have been registered as a table.
#teenagers = spark.sql("SELECT * FROM people WHERE age >= 13 AND age <= 19")

teenagers = spark.sql("SELECT name,sum(age) age FROM people WHERE age >= 13 AND age <= 19 group by name ")


# The results of SQL queries are RDDs and support all the normal RDD operations.
for teen in teenagers.collect():
#  print(teen.name,teen.age)
   print(teen.name,teen.age)

# We can also use functions instead of SQL queries:
#Agreegate = schemaPeople.groupBy("age").count().orderBy("age")
#Agreegate = teenagers.groupBy("age").count().orderBy("age")


#for agegrp in Agreegate.collect():
#    print(agegrp)




spark.stop()
