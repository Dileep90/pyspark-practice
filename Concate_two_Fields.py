from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('Concat').getOrCreate()
data =[('Dileep','Sharma'),('Naresh','Kumar')]
df = spark.createDataFrame(data).toDF("Co1","Co2")
concat =df.withColumn("Name",expr("co1||','||co2"))
concat.show()