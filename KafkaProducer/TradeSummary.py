from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, expr, to_timestamp, window


class TradeSummary():
    def __int__(self):
        self.base_dir=""

    def getSchema(self):
        from pyspark.sql.types import *
        return (
            StructType([
                StructField("CreatedTime", StringType),
                StructField("Type",StringType),
                StructField("Amount", DoubleType),
                StructField("BrokerCode", StringType)
            ])
        )

    def createSparkSession(self):
        spark = SparkSession.builder.master("local[1]") \
            .appName("KafkaConsumer").getOrCreate()
        return spark

    def readBronze(self):
        spark=self.createSparkSession()
        return spark.readStream.table("kafa_bz")

    def getTrade(self, kafka_df):
        return (
            kafka_df.select(from_json(col("value"),self.getSchema()).alias("value"))
            .select("value.*")
            .withColumn("CreatedTime", expr("to_timestamp(CreatedTime,'yyyy-MM-dd HH:mm:ss')"))
            .withColumn("Buy",expr("case when Type == 'BUY' then Amount else 0 end"))
            .withColumn("Sell", expr("case when Type == 'SELL' then Amount else 0 end"))

        )

    def getAggregate(self,trade_df):
        return (
            trade_df.withWatermark("CreatedTime", "30 minutes")
            .groupBy(window(col("CreatedTime"),"15 minutes"))
            .agg(sum("Buy").alias("TotalBuy"),
                 sum("Sell").alias("TotalSell"))
            .select("window.start","window.end","TotalBuy", "TotalSell")
        )
    def saveResults(self,result_df):
        print("\n Starting Silver Stream...")
        return (
            result_df.writeStream()
            .option("checkpointLocation", f"{self.base_dir}/checkpoint/trade_summary")
            .outputMode("append")
            .toTable("trade_summary")
        )

    def process(self):
        kafka_df=self.readBronze()
        trade_df=self.getTrade(kafka_df)
        result_df=self.getAggregate(trade_df)
        sQuery=self.saveResults(result_df)
        return sQuery

