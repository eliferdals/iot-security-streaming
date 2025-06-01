from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("pyspark_test").getOrCreate() 

bucket = "bitirme_proje3"
spark.conf.set("temporaryGcsBucket", bucket)

df = spark.read.json(f"gs://bitirme_proje3/")

df = df.filter(df["service"] == "mqtt")
#df = df.filter(col('service') == ('mqtt'))

df.write.format("bigquery") \
    .option("table", "Bitirme_projesi.mqtt") \
    .mode("overwrite").save()

#### pyspark --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 --jars=gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar