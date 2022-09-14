from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
import time

kafka_bootstrap_servers = "localhost:9092"
kafka_topic_name = "orders_topic"
customers_data_file_path = "file:////mnt/92D26AE0D26AC7D5/Python/chamath_kafka_with_pyspark/data_source/customers.csv"
mysql_driver_class = "com.mysql.jdbc.Driver"
mysql_table_name = "total_sales_by_source_state"
mysql_user_name = "root"
mysql_password = ""
mysql_jdbc_url = "jdbc:mysql://localhost:3306/sales_db?createDatabaseIfNotExist=true"

"""
here you create a spart session and because you are going to read kafka stream, you need to use
some jar files
"""
# mysql:mysql-connector-java:8.0.29
spark_session = (
	 SparkSession
	.builder
	.appName("pyspark_with_kafka")
	.config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1')
	.config("spark.jars", "jars/mysql-connector-java-8.0.29.jar")
	.getOrCreate())


"""
* here you read the kafka stream by subscribing given kafka topic/topics
* "startingOffsets" defines from where the events are going to consume like "earliest" means it
is possible to consume already consumed events and with "latest" means it only consumes event which
are yet to consumed
"""
orders_kafka_df = (
	spark_session
	.readStream
	.format("kafka")
	.option("kafka.bootstrap.servers", kafka_bootstrap_servers)
	.option("subscribe", kafka_topic_name)
	.option("startingOffsets", "latest")
	.load())

# with this you can check the data types forzzzzzzzzz
orders_kafka_df.printSchema()

def save_to_mysql_database(current_df, batch_id):
    processed_at = time.strftime("%Y-%m-%d %H:%M:%S")
    current_df_final = current_df \
        .withColumn("processed_at", lit(processed_at)) \
        .withColumn("batch_id", lit(batch_id))

    print(current_df_final.printSchema())
    print("Printing before Msql table save: " + str(batch_id))

    current_df_final \
        .write \
        .format("jdbc") \
        .option("driver", mysql_driver_class)\
        .option("url", mysql_jdbc_url) \
        .option("dbtable", mysql_table_name) \
        .option("user", mysql_user_name) \
        .option("password", mysql_password) \
        .mode("append") \
        .save()


# this will return new dataframe according to the given expression
# here we select value and timestamp fields from the pyspark dataframe
# here value field contains encoded json object and it is converted to string
orders_values_timestamp_df = orders_kafka_df.selectExpr("CAST(value AS STRING)", "timestamp")


# define a schema and this will be used to select fields from string in above dataframe
# https://spark.apache.org/docs/3.1.1/api/python/reference/api/pyspark.sql.types.StructType.html?highlight=structtype#pyspark.sql.types.StructType
orders_schema = StructType() \
    .add("ID", StringType()) \
    .add("Created At", StringType()) \
    .add("Discount", StringType()) \
    .add("Product ID", StringType()) \
    .add("Quantity", StringType()) \
    .add("Subtotal", StringType()) \
    .add("Tax", StringType()) \
    .add("Total", StringType()) \
    .add("User ID", StringType())


# https://spark.apache.org/docs/3.1.1/api/python/reference/api/pyspark.sql.functions.from_json.html?highlight=from_json#pyspark.sql.functions.from_json
# 
# select() execute select expression for given fields(in the schema) and return the resulting dataframe
orders_df = orders_values_timestamp_df.select(
	from_json(col("value"), orders_schema).alias("orders"), "timestamp")

orders_df = orders_df.select("orders.*", "timestamp")

customers_df = spark_session.read.csv(customers_data_file_path, header=True, inferSchema=True)

customers_orders_df = orders_df.join(
	customers_df,
	orders_df["User ID"]==customers_df["Customer_ID"],
	how="inner")

customers_orders_grouped_df = (customers_orders_df.groupBy("Source", "State")
	.agg({"Total": "sum"})
	.select("Source", "State", col("sum(Total)").alias("total_sum_amount"))
	)

"""
* here you can write the kafka stream to console
* there are several output format like "console", "kafka", "foreach", "file" and "memory"
* output formats : https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#output-sinks
* "outputMode" defines the way the data is written to the output(append, complete, update)
* with start() we can start the streaming computation
* with awaitTermination(), query is blocked until it is got terminated
* here you can find for info about managing streaming queries
 https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#managing-streaming-queries
"""
(customers_orders_grouped_df
	.writeStream
	.trigger(processingTime="3 seconds")
	.outputMode("update")
	.foreachBatch(save_to_mysql_database)
	.start()
	.awaitTermination()
	)




# # processingTime is used to define the time interval that is used to looking for new data
# # with 'foreachBatch' data is processed batch by batch and with 'foreach' it is done by row by row
# (orders_df.writeStream
# 	.foreachBatch(lambda df,id: print(df.show()))
# 	.outputMode("append")
# 	.trigger(processingTime="3 seconds")
# 	.start()
# 	.awaitTermination())

# # (updated_df.writeStream
# # 	.foreachBatch(write_func)
# # 	.outputMode("append")
# # 	.trigger(processingTime="3 seconds")
# # 	.start()
# # 	.awaitTermination())