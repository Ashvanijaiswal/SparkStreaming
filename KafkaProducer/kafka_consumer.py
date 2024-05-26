from pyspark.sql.window import Window
from pyspark.sql.functions import *

from pyspark.sql import SparkSession

class KafkaConsumer:
    def __init__(self):
        self.topic = "invoices"
        self.base_dir = "G:\Apache-Spark-and-Databricks-Stream-Processing-in-Lakehouse-main\KafkaProducer\data"
        self.conf = {
            "kafka.sasl.jaas.config": 'org.apache.kafka.common.security.plain.PlainLoginModule required username="OJ52GHVCYGEETXHI" password="iA0WT8bwzz8ghK3vHbml6yWtx7e/53YQQ9+hDCcatbH9MUc7LkWpQDeKWnY1QO/s";',
            "kafka.sasl.mechanism": "PLAIN",
            "kafka.security.protocol" : "SASL_SSL",
            "kafka.bootstrap.servers": 'pkc-12576z.us-west2.gcp.confluent.cloud:9092',
            "subscribe": 'invoices',
            }
    def createSparkSession(self):

        spark=SparkSession.builder.master("local[1]") \
            .appName("KafkaConsumer").getOrCreate()
        return spark

    def getSchema(self):
        return """InvoiceNumber string, CreatedTime bigint, StoreID string, PosID string, CashierID string,
                 CustomerType string, CustomerCardNo string, TotalAmount double, NumberOfItems bigint, 
                 PaymentMethod string, TaxableAmount double, CGST double, SGST double, CESS double, 
                 DeliveryType string,
                 DeliveryAddress struct<AddressLine string, City string, ContactNumber string, PinCode string, 
                 State string>,
                 InvoiceLineItems array<struct<ItemCode string, ItemDescription string, 
                     ItemPrice double, ItemQty bigint, TotalValue double>>
             """

    def readDataFromKafka(self, startingTimestamp = 1):
        spark = self.createSparkSession()
        df = spark \
            .read \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "pkc-12576z.us-west2.gcp.confluent.cloud:9092")\
            .option("kafka.security.protocol","SASL_SSL" )\
            .option("kafka.sasl.mechanism", "PLAIN")\
            .option("kafka.sasl.jaas.config","org.apache.kafka.common.security.plain.PlainLoginModule required username='OJ52GHVCYGEETXHI' password='iA0WT8bwzz8ghK3vHbml6yWtx7e/53YQQ9+hDCcatbH9MUc7LkWpQDeKWnY1QO/s';")\
            .option("topic", "invoices")\
            .option("maxoffsetsPerTrigger", 10) \
            .option("startingTimestamp" , startingTimestamp)\
            .load()
        return df

    def getInvoices(self, kafka_df):
        return  (kafka_df.select(kafka_df.key.cast("string").alias("key"),
                                 from_json(kafka_df.value.cast("value").alias("value"), self.getSchema()),
                                 "topic", "timestamp"))

    def process(self, startingTime = 1):
        print("starting bronze ingestion ...")
        kafka_df = self.readDataFromKafka(startingTime)
        invoices_df = self.getInvoices(kafka_df)
        sQuery = (invoices_df.writeStream \
                  .option("checkpointLocation",self.base_dir )\
                  .outputMode("append")\
                  .toTable("invoices_bz"))

if __name__ == "__main__":
    invoice_consumer  = KafkaConsumer()
    invoice_consumer.process()
