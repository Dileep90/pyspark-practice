from pyspark.sql import SparkSession
spark = SparkSession.builder.master('Dileep-practice').appName('How to create SparkSession').getOrCreate()