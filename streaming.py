from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, FloatType, StringType, IntegerType, FloatType

bucket = "bitirme_proje3"
spark.conf.set('temporaryGcsBucket', bucket)
spark.conf.set('parentProject', 'massive-hub-423119-c8')

{
	{"id.orig_p": 55757, 
     "id.resp_p": 1883, 
     "proto": "tcp", 
     "service": "mqtt", 
     "flow_duration": 61.968777, 
     "fwd_pkts_tot": 10, 
     "bwd_pkts_tot": 6, 
     "fwd_data_pkts_tot": 3, 
     "bwd_data_pkts_tot": 4, 
     "fwd_pkts_per_sec": 0.161372, 
     "bwd_pkts_per_sec": 0.096823, 
     "flow_pkts_per_sec": 0.258195, 
     "down_up_ratio": 0.6, 
     "fwd_header_size_tot": 328, 
     "fwd_header_size_min": 32, 
     "fwd_header_size_max": 40, 
     "bwd_header_size_tot": 200, 
     "bwd_header_size_min": 32, 
     "bwd_header_size_max": 40, 
     "flow_FIN_flag_count": 0, 
     "flow_SYN_flag_count": 2, 
     "flow_RST_flag_count": 1, 
     "fwd_PSH_flag_count": 3, 
     "bwd_PSH_flag_count": 4, 
     "flow_ACK_flag_count": 15, 
     "fwd_URG_flag_count": 0, 
     "bwd_URG_flag_count": 0, 
     "flow_CWR_flag_count": 0, 
     "flow_ECE_flag_count": 0, 
     "fwd_pkts_payload.min": 0.0, 
     "fwd_pkts_payload.max": 33.0, 
     "fwd_pkts_payload.tot": 78.0, 
     "fwd_pkts_payload.avg": 7.8, 
     "fwd_pkts_payload.std": 12.934021, 
     "bwd_pkts_payload.min": 0.0, 
     "bwd_pkts_payload.max": 23.0, 
     "bwd_pkts_payload.tot": 36.0, 
     "bwd_pkts_payload.avg": 6.0, 
     "bwd_pkts_payload.std": 8.602325, 
     "flow_pkts_payload.min": 0.0, 
     "flow_pkts_payload.max": 33.0, 
     "flow_pkts_payload.tot": 114.0, 
     "flow_pkts_payload.avg": 7.125, 
     "flow_pkts_payload.std": 11.218289, 
     "fwd_iat.min": 252.962112, 
     "fwd_iat.max": 59909150.123596, 
     "fwd_iat.tot": 61968777.179718, 
     "fwd_iat.avg": 6885419.686635002, 
     "fwd_iat.std": 19889026.298581, 
     "bwd_iat.min": 64.134598, 
     "bwd_iat.max": 1592000.961304, 
     "bwd_iat.tot": 1903043.031693, 
     "bwd_iat.avg": 380608.606339, 
     "bwd_iat.std": 681601.3467, 
     "flow_iat.min": 64.134598, 
     "flow_iat.max": 59909150.123596, 
     "flow_iat.tot": 61968777.179718, 
     "flow_iat.avg": 4131251.811981, 
     "flow_iat.std": 15434786.23245, 
     "payload_bytes_per_second": 1.839636, 
     "fwd_subflow_pkts": 3.333333, 
     "bwd_subflow_pkts": 2.0, 
     "fwd_subflow_bytes": 26.0, 
     "bwd_subflow_bytes": 12.0, 
     "fwd_bulk_bytes": 0.0, 
     "bwd_bulk_bytes": 0.0, 
     "fwd_bulk_packets": 0.0, 
     "bwd_bulk_packets": 0.0, 
     "fwd_bulk_rate": 0.0, 
     "bwd_bulk_rate": 0.0, 
     "active.min": 2059627.056122, 
     "active.max": 2059627.056122, 
     "active.tot": 2059627.056122, 
     "active.avg": 2059627.056122, 
     "active.std": 0.0, 
     "idle.min": 59909150.123596, 
     "idle.max": 59909150.123596, 
     "idle.tot": 59909150.123596, 
     "idle.avg": 59909150.123596, 
     "idle.std": 0.0, 
     "fwd_init_window_size": 64240, 
     "bwd_init_window_size": 26847, 
     "fwd_last_window_size": 502, 
     "Attack_type": "MQTT_Publish"}
}

spark = SparkSession.builder.appName('streaming').getOrCreate()

schema = StructType([
    StructField('id.orig_p', StringType(), True),
    StructField('id.resp_p', StringType(), True),
    StructField('proto', StringType(), True),
    StructField('service', StringType(), True),
    StructField('flow_duration', StringType(), True),
    StructField('fwd_pkts_tot', StringType(), True),
    StructField('bwd_pkts_tot', StringType(), True),
    StructField('fwd_data_pkts_tot', StringType(), True),
    StructField('bwd_data_pkts_tot', StringType(), True),
    StructField('fwd_pkts_per_sec', StringType(), True),
    StructField('bwd_pkts_per_sec', StringType(), True),
    StructField('flow_pkts_per_sec', StringType(), True),
    StructField('down_up_ratio', StringType(), True),
    StructField('fwd_header_size_tot', StringType(), True),
    StructField('fwd_header_size_min', StringType(), True),
    StructField('fwd_header_size_max', StringType(), True),
    StructField('bwd_header_size_tot', StringType(), True),
    StructField('bwd_header_size_min', StringType(), True),
    StructField('bwd_header_size_max', StringType(), True),
    StructField('flow_FIN_flag_count', StringType(), True),
    StructField('flow_SYN_flag_count', StringType(), True),
    StructField('flow_RST_flag_count', StringType(), True),
    StructField('fwd_PSH_flag_count', StringType(), True),
    StructField('bwd_PSH_flag_count', StringType(), True),
    StructField('flow_ACK_flag_count', StringType(), True),
    StructField('fwd_URG_flag_count', StringType(), True),
    StructField('bwd_URG_flag_count', StringType(), True),
    StructField('flow_CWR_flag_count', StringType(), True),
    StructField('flow_ECE_flag_count', StringType(), True),
    StructField('fwd_pkts_payload.min', StringType(), True),
    StructField('fwd_pkts_payload.max', StringType(), True),
    StructField('fwd_pkts_payload.tot', StringType(), True),
    StructField('fwd_pkts_payload.avg', StringType(), True),
    StructField('fwd_pkts_payload.std', StringType(), True),
    StructField('bwd_pkts_payload.min', StringType(), True),
    StructField('bwd_pkts_payload.max', StringType(), True),
    StructField('bwd_pkts_payload.tot', StringType(), True),
    StructField('bwd_pkts_payload.avg', StringType(), True),
    StructField('bwd_pkts_payload.std', StringType(), True),
    StructField('flow_pkts_payload.min', StringType(), True),
    StructField('flow_pkts_payload.max', StringType(), True),
    StructField('flow_pkts_payload.tot', StringType(), True),
    StructField('flow_pkts_payload.avg', StringType(), True),
    StructField('flow_pkts_payload.std', StringType(), True),
    StructField('fwd_iat.min', StringType(), True),
    StructField('fwd_iat.max', StringType(), True),
    StructField('fwd_iat.tot', StringType(), True),
    StructField('fwd_iat.avg', StringType(), True),
    StructField('fwd_iat.std', StringType(), True),
    StructField('bwd_iat.min', StringType(), True),
    StructField('bwd_iat.max', StringType(), True),
    StructField('bwd_iat.tot', StringType(), True),
    StructField('bwd_iat.avg', StringType(), True),
    StructField('bwd_iat.std', StringType(), True),
    StructField('flow_iat.min', StringType(), True),
    StructField('flow_iat.max', StringType(), True),
    StructField('flow_iat.tot', StringType(), True),
    StructField('flow_iat.avg', StringType(), True),
    StructField('flow_iat.std', StringType(), True),
    StructField('payload_bytes_per_second', StringType(), True),
    StructField('fwd_subflow_pkts', StringType(), True),
    StructField('bwd_subflow_pkts', StringType(), True),
    StructField('fwd_subflow_bytes', StringType(), True),
    StructField('bwd_subflow_bytes', StringType(), True),
    StructField('fwd_bulk_bytes', StringType(), True),
    StructField('bwd_bulk_bytes', StringType(), True),
    StructField('fwd_bulk_packets', StringType(), True),
    StructField('bwd_bulk_packets', StringType(), True),
    StructField('fwd_bulk_rate', StringType(), True),
    StructField('bwd_bulk_rate', StringType(), True),
    StructField('active.min', StringType(), True),
    StructField('active.max', StringType(), True),
    StructField('active.tot', StringType(), True),
    StructField('active.avg', StringType(), True),
    StructField('active.std', StringType(), True),
    StructField('idle.min', StringType(), True),
    StructField('idle.max', StringType(), True),
    StructField('idle.tot', StringType(), True),
    StructField('idle.avg', StringType(), True),
    StructField('idle.std', StringType(), True),
    StructField('fwd_init_window_size', StringType(), True),
    StructField('bwd_init_window_size', StringType(), True),
    StructField('fwd_last_window_size', StringType(), True),
    StructField('Attack_type', StringType(), True) 
    ])

kafkaDF = spark.readStream.format('kafka')\
    .option('kafka.bootstrap.servers', '35.188.57.91:9092')\
    .option('subscribe', 'proto').load()

activationDF = kafkaDF.select(from_json(kafkaDF['value'].cast('string'), schema).alias('activation')).select('activation.*')

df = (activationDF
    .withColumn('id.orig_p', col('id.orig_p').cast(IntegerType()))
    .withColumn('id.resp_p', col('id.resp_p').cast(IntegerType()))
    .withColumn('proto', col('proto').cast(StringType()))
    .withColumn('service', col('service').cast(StringType()))
    .withColumn('flow_duration', col('flow_duration').cast(FloatType()))
    .withColumn('fwd_pkts_tot', col('fwd_pkts_tot').cast(IntegerType()))
    .withColumn('bwd_pkts_tot', col('bwd_pkts_tot').cast(IntegerType()))
    .withColumn('fwd_data_pkts_tot', col('fwd_data_pkts_tot').cast(IntegerType()))
    .withColumn('bwd_data_pkts_tot', col('bwd_data_pkts_tot').cast(IntegerType()))
    .withColumn('fwd_pkts_per_sec', col('fwd_pkts_per_sec').cast(FloatType()))
    .withColumn('bwd_pkts_per_sec', col('bwd_pkts_per_sec').cast(FloatType()))
    .withColumn('flow_pkts_per_sec', col('flow_pkts_per_sec').cast(FloatType()))
    .withColumn('down_up_ratio', col('down_up_ratio').cast(FloatType()))
    .withColumn('fwd_header_size_tot', col('fwd_header_size_tot').cast(IntegerType()))
    .withColumn('fwd_header_size_min', col('fwd_header_size_min').cast(IntegerType()))
    .withColumn('fwd_header_size_max', col('fwd_header_size_max').cast(IntegerType()))
    .withColumn('bwd_header_size_tot', col('bwd_header_size_tot').cast(IntegerType()))
    .withColumn('bwd_header_size_min', col('bwd_header_size_min').cast(IntegerType()))
    .withColumn('bwd_header_size_max', col('bwd_header_size_max').cast(IntegerType()))
    .withColumn('flow_FIN_flag_count', col('flow_FIN_flag_count').cast(IntegerType()))
    .withColumn('flow_SYN_flag_count', col('flow_SYN_flag_count').cast(IntegerType()))
    .withColumn('flow_RST_flag_count', col('flow_RST_flag_count').cast(IntegerType()))
    .withColumn('fwd_PSH_flag_count', col('fwd_PSH_flag_count').cast(IntegerType()))
    .withColumn('bwd_PSH_flag_count', col('bwd_PSH_flag_count').cast(IntegerType()))
    .withColumn('flow_ACK_flag_count', col('flow_ACK_flag_count').cast(IntegerType()))
    .withColumn('fwd_URG_flag_count', col('fwd_URG_flag_count').cast(IntegerType()))
    .withColumn('bwd_URG_flag_count', col('bwd_URG_flag_count').cast(IntegerType()))
    .withColumn('flow_CWR_flag_count', col('flow_CWR_flag_count').cast(IntegerType()))
    .withColumn('flow_ECE_flag_count', col('flow_ECE_flag_count').cast(IntegerType()))
    .withColumn('fwd_pkts_payload.min', col('fwd_pkts_payload.min').cast(FloatType()))
    .withColumn('fwd_pkts_payload.max', col('fwd_pkts_payload.max').cast(FloatType()))
    .withColumn('fwd_pkts_payload.tot', col('fwd_pkts_payload.tot').cast(FloatType()))
    .withColumn('fwd_pkts_payload.avg', col('fwd_pkts_payload.avg').cast(FloatType()))
    .withColumn('fwd_pkts_payload.std', col('fwd_pkts_payload.std').cast(FloatType()))
    .withColumn('bwd_pkts_payload.min', col('bwd_pkts_payload.min').cast(FloatType()))
    .withColumn('bwd_pkts_payload.max', col('bwd_pkts_payload.max').cast(FloatType()))
    .withColumn('bwd_pkts_payload.tot', col('bwd_pkts_payload.tot').cast(FloatType()))
    .withColumn('bwd_pkts_payload.avg', col('bwd_pkts_payload.avg').cast(FloatType()))
    .withColumn('bwd_pkts_payload.std', col('bwd_pkts_payload.std').cast(FloatType()))
    .withColumn('flow_pkts_payload.min', col('flow_pkts_payload.min').cast(FloatType()))
    .withColumn('flow_pkts_payload.max', col('flow_pkts_payload.max').cast(FloatType()))
    .withColumn('flow_pkts_payload.tot', col('flow_pkts_payload.tot').cast(FloatType()))
    .withColumn('flow_pkts_payload.avg', col('flow_pkts_payload.avg').cast(FloatType()))
    .withColumn('flow_pkts_payload.std', col('flow_pkts_payload.std').cast(FloatType()))
    .withColumn('fwd_iat.min', col('fwd_iat.min').cast(FloatType()))
    .withColumn('fwd_iat.max', col('fwd_iat.max').cast(FloatType()))
    .withColumn('fwd_iat.tot', col('fwd_iat.tot').cast(FloatType()))
    .withColumn('fwd_iat.avg', col('fwd_iat.avg').cast(FloatType()))
    .withColumn('fwd_iat.std', col('fwd_iat.std').cast(FloatType()))
    .withColumn('bwd_iat.min', col('bwd_iat.min').cast(FloatType()))
    .withColumn('bwd_iat.max', col('bwd_iat.max').cast(FloatType()))
    .withColumn('bwd_iat.tot', col('bwd_iat.tot').cast(FloatType()))
    .withColumn('bwd_iat.avg', col('bwd_iat.avg').cast(FloatType()))
    .withColumn('bwd_iat.std', col('bwd_iat.std').cast(FloatType()))
    .withColumn('flow_iat.min', col('flow_iat.min').cast(FloatType()))
    .withColumn('flow_iat.max', col('flow_iat.max').cast(FloatType()))
    .withColumn('flow_iat.tot', col('flow_iat.tot').cast(FloatType()))
    .withColumn('flow_iat.avg', col('flow_iat.avg').cast(FloatType()))
    .withColumn('flow_iat.std', col('flow_iat.std').cast(FloatType()))
    .withColumn('payload_bytes_per_second', col('payload_bytes_per_second').cast(FloatType()))
    .withColumn('fwd_subflow_pkts', col('fwd_subflow_pkts').cast(FloatType()))
    .withColumn('bwd_subflow_pkts', col('bwd_subflow_pkts').cast(FloatType()))
    .withColumn('fwd_subflow_bytes', col('fwd_subflow_bytes').cast(FloatType()))
    .withColumn('bwd_subflow_bytes', col('bwd_subflow_bytes').cast(FloatType()))
    .withColumn('fwd_bulk_bytes', col('fwd_bulk_bytes').cast(FloatType()))
    .withColumn('bwd_bulk_bytes', col('bwd_bulk_bytes').cast(FloatType()))
    .withColumn('fwd_bulk_packets', col('fwd_bulk_packets').cast(FloatType()))
    .withColumn('bwd_bulk_packets', col('bwd_bulk_packets').cast(FloatType()))
    .withColumn('fwd_bulk_rate', col('fwd_bulk_rate').cast(FloatType()))
    .withColumn('bwd_bulk_rate', col('bwd_bulk_rate').cast(FloatType()))
    .withColumn('active.min', col('active.min').cast(FloatType()))
    .withColumn('active.max', col('active.max').cast(FloatType()))
    .withColumn('active.tot', col('active.tot').cast(FloatType()))
    .withColumn('active.avg', col('active.avg').cast(FloatType()))
    .withColumn('active.std', col('active.std').cast(FloatType()))
    .withColumn('idle.min', col('idle.min').cast(FloatType()))
    .withColumn('idle.max', col('idle.max').cast(FloatType()))
    .withColumn('idle.tot', col('idle.tot').cast(FloatType()))
    .withColumn('idle.avg', col('idle.avg').cast(FloatType()))
    .withColumn('idle.std', col('idle.std').cast(FloatType()))
    .withColumn('fwd_init_window_size', col('fwd_init_window_size').cast(FloatType()))
    .withColumn('bwd_init_window_size', col('bwd_init_window_size').cast(FloatType()))
    .withColumn('fwd_last_window_size', col('fwd_last_window_size').cast(StringType()))
    )

df = df.filter(col('fwd_pkts_tot') > 8)

modelCountQuery = df.writeStream.outputMode('append').format('console').start().awaitTermination()

df.show()

##df.writeStream.format('console').outputMode('append').start().awaitTermination()

###pyspark --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 --jars gs://spark-lib/bigquery/spark-bigquery/spark-bigquery-latest_2.12.jar
#### pyspark --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 --jars=gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar

