package com.onemarket.metrics

import java.nio.charset.StandardCharsets
import java.util.UUID
import com.samelamin.spark.bigquery._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, current_timestamp, input_file_name, udf}
import scala.collection.Seq
import scala.reflect.api.materializeTypeTag


object NordstromTransactions {

  def deriveChannel = udf((x: String) => {
    x.toLowerCase match {
      case "n.com" => "ONLINE"
      case "full line" => "IN STORE"
      case "fl canada" => "CANADA"
      case _ => x
    }
  })

  def deriveCountry = udf((x: String) => {
    x.toLowerCase match {
      case "n.com" => "ONLINE"
      case "full line" => "USA"
      case "fl canada" => "CANADA"
      case _ => x
    }
  })

  def getDataPartyId = udf((key: String) => {
    UUID.nameUUIDFromBytes(key.toLowerCase.getBytes(StandardCharsets.ISO_8859_1)).toString
  })

  def parseDate = udf((x: String) => {
    x.slice(53, 63)
  })

  def getYear = udf((x: String) => {
    x.slice(53, 57)
  })

  def getMonth = udf((x: String) => {
    x.slice(58, 60)
  })

  def getDay = udf((x: String) => {
    x.slice(61, 63)
  })

  def main(args: Array[String]) {
    val spark = SparkSession.builder.appName("Nordstrom Transactions").getOrCreate()

    val gcsOnboardPath = "vendor-tdm-poc/raw/daily/nordstrom_transactions"
    val fullOnboardPath = s"gs://$gcsOnboardPath/2016/*/*/*.gz"

    //val certFolderPath = s"gs://dm-data-cert-poc-tier1/vendor-tdm/nordstrom_transactions/"
    
    val certFolderPath = s"gs://testbucketdeletemeuseless/vendor-tdm/nordstrom_transactions1/"

    val taxonomyColumns = Seq("shopper_id", "transaction_id", "channel",
      "store_id", "product_id", "quantity", "transaction_total",
      "item_discount", "item_total", "event_time")

    val df = spark.read.option("header", "false").option("delimiter", "|").csv(fullOnboardPath)
    val dfRenamed = df.toDF(taxonomyColumns: _*)
    val dfWithMiscColumns = dfRenamed.withColumn("party_id", getDataPartyId(col("shopper_id"))).withColumn("file_path", input_file_name).withColumn("date_processed", current_timestamp)
    val dfDeriviedColumns = dfWithMiscColumns.withColumn("derived_channel", deriveChannel(col("channel"))).withColumn("derived_country", deriveCountry(col("channel")))
    val dfWithDates = dfDeriviedColumns.withColumn("year", getYear(col("file_path"))).withColumn("month", getMonth(col("file_path"))).withColumn("day", getDay(col("file_path")))
    dfWithDates.write.partitionBy("year", "month", "day")
      .option("header", "true")
      .option("delimiter", "\t")
      .mode("overwrite")
      .csv(certFolderPath)
  }
}
