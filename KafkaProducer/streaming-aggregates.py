from pyspark.sql import SparkSession


class StreamAggregate:
    def __init__(self):
        self.base_dir="data/"

    def createSparkSession(self):
        spark = SparkSession.builder.master("local[1]") \
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

    def readInvoices(self):
        from pyspark.sql.functions import input_file_name
        spark=self.createSparkSession()
        return (spark.readStream.format("json").schema(self.getSchema()).
                 load("data/invoices.json")
                .withColumn("InputFile",input_file_name()))

    def process(self):
        print(f"\n starting Bronze Stream...")
        invoice_df=self.readInvoices()
        sQuery = (invoice_df.writeStream
                  .queryName("bronze-ingestion")
                  .option("checkpointLocation", f"{self.base_dir}/checkpoint/invoices_bz")
                  .outputMode("append")
                  .toTable("invoices_bz")
                  )
        print("Done")
        return sQuery

class Gold():
    def __init__(self):
        self.base_dir="./data/"

    def createSparkSession(self):
        spark = SparkSession.builder.master("local[1]") \
            .appName("KafkaConsumer").getOrCreate()
        return spark
    def readBronze(self):
        spark=self.createSparkSession()
        return spark.readStream.table("invoices_bz")

    def getAggregates(self,invoices_df):
        from pyspark.sql.functions import sum,expr
        return (
            invoices_df.groupBy("customerCardNo")
            .agg(sum("TotalAmount").alias("TotalAmount"),
                 sum(expr("TotalAmount*0.02")).alias("TotalPoints"))

        )

    def upsert(self,rewards_df,batch_id):
        rewards_df.createOrReplaceTempView("customer_rewards_df_temp_view")
        merge_stmt="""
            MERGE INTO customer_rewards t
            USING customer_rewards_df_temp_view s
            ON s.CustomerCardNo=t.customer_rewards_df_temp_view
            WHEN MATCHED THEN
            UPDATE SET t.TotalAmount=s.TotalAmount, t.TotalPoints=s.TotalPoints
            WHEN NOT MATCHED THEN
            INSERT *
        """
        rewards_df._jdf.sparkSession().sql(merge_stmt)

    # incremental load
    # to solve the problem of complete mode , update mode is used
    # only record that are either new or modified
    def saveResults(self,results_df):
        print("\n Startig Silver Stream...")
        return (
            results_df.writeStream.queryName("gold-update")
            .option("checkpointLocation", f"{self.base_dir}/checkpoint/customer_rewards")
            .outputMode("update")
            .foreachBatch(self.upsert)
            .start()
        )

    def process(self):
        invoice_df = self.readBronze()
        aggregate_df = self.getAggregates(invoice_df)
        sQuery = self.saveResults(aggregate_df)
        return sQuery

if __name__=='__main__':
    print("process start for bronze")
    obj=StreamAggregate()
    bzQuery=obj.process()
    print("processEnd for bronze")

    print("gold process start")
    obj = Gold()
    gdQuery = obj.process()
    print("gold process end")

    # bzQuery.stop()
    # gdQuery.stop()

