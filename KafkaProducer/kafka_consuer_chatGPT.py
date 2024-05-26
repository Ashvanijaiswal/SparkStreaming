from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("KafkaStreamReader") \
    .getOrCreate()

# Kafka source configuration
kafka_bootstrap_servers = 'pkc-12576z.us-west2.gcp.confluent.cloud:9092'
kafka_topic = 'invoices'
kafka_username = 'OJ52GHVCYGEETXHI'
kafka_password = 'iA0WT8bwzz8ghK3vHbml6yWtx7e/53YQQ9+hDCcatbH9MUc7LkWpQDeKWnY1QO/s'

# Configure Kafka properties
kafka_properties = {
    'kafka.bootstrap.servers': kafka_bootstrap_servers,
    'subscribe': kafka_topic,
    'kafka.security.protocol': 'SASL_SSL',
    'kafka.sasl.mechanism': 'PLAIN',
    'kafka.sasl.jaas.config': f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{kafka_username}" password="{kafka_password}";'
}

# Read from Kafka
df = spark.readStream \
    .format("kafka") \
    .options(**kafka_properties) \
    .load()

# Start the streaming query
query = df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()
