from pyspark.sql import SparkSession
from pyspark.sql.functions import col
spark=SparkSession.builder.appName('Order').getOrCreate()
#orders = spark.read.schema(schema).csv(f'/user/{username}/retail_db_pipe/orders', sep='|')
df_date=spark.read.format('csv').options(inferSchema='True').load('/FileStore/tables/orders')
#schama=('id' int,'date' date,'balance' int, 'status' string)
#df_date.show()
df_modi= df_date.select(col('_c0'),date_format('_c1','DDMONYYYY'))
df_modi.printSchema() #convert into string
cols= [col('_c0'),date_format('_c1','yyyyMMdd').alias('date')]
df_date.select(cols).show()