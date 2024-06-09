from pyspark.sql import SparkSession

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
    def saveResults(self,results_df):
        print("\n Startig Silver Stream...")
        return (
            results_df.writeStream.queryName("gold-update")
            .option("checkpointLocation", f"{self.base_dir}/checkpoint/customer_rewards")
            .outputMode("complete")
            .toTable("customer_rewards")
        )

    def process(self):
        invoice_df = self.readBronze()
        aggregate_df = self.getAggregates(invoice_df)
        sQuery = self.saveResults(aggregate_df)
        return sQuery

if __name__=='__main__':
    print("gold process start")
    obj=Gold()
    sQuery=obj.process()
    print("gold process end")
    sQuery.stop()