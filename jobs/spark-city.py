from pyspark.sql import SparkSession
# from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from config import configuration

def main():
    spark = SparkSession.builder.appName("SmartCityStreaming") \
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.0,"
            "org.apache.hadoop:hadoop-aws:3.3.1,"
            "com.amazonaws:aws-java-sdk:1.11.469")\
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")\
    .config("spark.hadoop.fs.s3a.access.key",configuration.get('AWS_ACCESS_KEY'))\
    .config("spark.hadoop.fs.s3a.secret.key",configuration.get('AWS_SECRET_KEY'))\
    .config('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider')\
    .getOrCreate()

    #Adjust the log level to minimize the console output on executors
    spark.sparkContext.setLogLevel('WARN')

    #vechile schema
    vechileSchema = StructType([
        StructField("id", StringType(), True),
        StructField("deviceId", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("location", StringType(), True),
        StructField("speed", DoubleType(), True),
        StructField("direction", StringType(), True),
        StructField("make", StringType(), True),
        StructField("model", StringType(), True),
        StructField("year", IntegerType(),True),
        StructField("fuelType", StringType(),True)
    ])

    gpsSchema = StructType([
    StructField("id", StringType(), True),
    StructField("deviceId", StringType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("speed", DoubleType(), True),
    StructField("direction", StringType(), True),
    StructField("vechicleType", StringType(),True)
    ])

    trafficSchema = StructType([
    StructField("id", StringType(), True),
    StructField("deviceId", StringType(), True),
    StructField("cameraId", StringType(), True),
    StructField("location", StringType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("snapshot", StringType(),True)
    ])

    weatherSchema = StructType([
        StructField("id", StringType(), True),
        StructField("deviceId", StringType(), True),
        StructField("location", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("temperature", DoubleType(), True),
        StructField("weatherCondition", StringType(), True),
        StructField("precipitation", DoubeType(), True),
        StructField("windSpeed", DoubleType(), True),
        StructField("humidty", IntegerType(),True),
        StructField("airQualityIndex", DoubleType(),True)
    ])

    emergencySchema = StructType([
        StructField("id", StringType(), True),
        StructField("deviceId", StringType(), True),
        StructField("incidentId", StringType(), True),
        StructField("type", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("location", StringType(), True),
        StructField("status", StringType(), True),
        StructField("description", StringType(), True)
    ])

    def read_kafka_topic(topic, schema):
        return (spark.readStream
                .format('kafka')
                .option('kafka.bootstrap.servers', 'broker:29092')
                .option('subscribe', topic)
                .option('startingOffsets', 'earliest')
                .load()
                .selectExpr('CAST(value as STRING)')
                .select(from_json(col('value'), schema).alias('data'))
                .select('data.*')
                .withWatermark('timestamp', '2 minutes'))

    def streamWriter(input: DataFrame, checkpointFolder, output):
        return (input.writeSteam
                .format('parquet')
                .option('checkpointLocation', checkpointFolder)
                .option('path', output)
                .outputMode('append')
                .start())
    

    vehicleDF = read_kafka_topic('vehicle_data', vechileSchema).alias('vehicle')
    gpsDF = read_kafka_topic('gps_data', gpsSchema).alias('gps')
    trafficDF = read_kafka_topic('traffic_data', trafficSchema).alias('traffic')
    weatherDF = read_kafka_topic('weather_data', weatherSchema).alias('weather')
    emergencyDF = read_kafka_topic('emergency_data', emergencySchema).alias('emergency')
    
    query1 = streamWriter(vehicleDF,'s3a://spark-steaming-data/checkpoint/vehicle_data', 's3a://spark-steaming-data/data/vehicle_data')
    query2 = streamWriter(gpsDF,'s3a://spark-steaming-data/checkpoint/gps_data', 's3a://spark-steaming-data/data/gps_data')
    query3 = streamWriter(trafficDF,'s3a://spark-steaming-data/checkpoint/traffic_data', 's3a://spark-steaming-data/data/traffic_data')
    query4 = streamWriter(weatherDF,'s3a://spark-steaming-data/checkpoint/weather_data', 's3a://spark-steaming-data/data/weather_data')
    query5 = streamWriter(emergencyDF,'s3a://spark-steaming-data/checkpoint/emergency_data', 's3a://spark-steaming-data/data/emergency_data')

    query5.awaitTermination()



if __name__ == "__main__":
    main()

