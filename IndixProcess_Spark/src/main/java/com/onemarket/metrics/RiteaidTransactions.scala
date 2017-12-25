package com.onemarket.metrics

import java.nio.charset.StandardCharsets
import java.util.UUID
import com.samelamin.spark.bigquery._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, current_timestamp, input_file_name, udf}
import scala.collection.Seq
import scala.reflect.api.materializeTypeTag

object RiteaidTransactions {
  def getDataPartyId = udf((key: String) => {
      UUID.nameUUIDFromBytes(key.toLowerCase.getBytes(StandardCharsets.ISO_8859_1)).toString

    
  })


  /*def parseDate = udf((x: String) => {
    x.slice(53, 63)
  })*/

  def getYear = udf((x: String) => {
    //x.slice(53, 57)
     x.split("/").takeRight(4).slice(0,1).toString()
  })

  def getMonth = udf((x: String) => {
    //x.slice(58, 60)
     x.split("/").takeRight(3).slice(0,1).toString()
  })

  def getDay = udf((x: String) => {
   // x.slice(61, 63)
     x.split("/").takeRight(2).slice(0,1).toString()
  })

  def main(args: Array[String]) {
    val spark = SparkSession.builder.appName("Riteaid Transactions").getOrCreate()

    val gcsOnboardPath = "vendor-tdm-poc/raw/daily/riteaid_transactions"
    
    val fullOnboardPath = s"gs://$gcsOnboardPath/2017/*/*/*.gz"

    //val certFolderPath = s"gs://dm-data-cert-poc-tier1/vendor-tdm/nordstrom_transactions/"
    
    val certFolderPath = s"gs://dm-data-cert-poc-tier1/vendor-tdm/riteaid_transactions/1/0"
    val failureFolderPath = s"gs://dm-data-failure-poc-tier1/vendor-tdm/riteaid_transactions/1/0"

    val taxonomyColumns = Seq("customer","transid","date","time","store","itemtype","upc_mask","uncode","sales",
         "card_disc","ad_disc","other_disc","xretail","units","card","isrxbskt","ad_event","ad_version","ad_pg_id")


    val df = spark.read.option("header", "true").option("delimiter", "|").csv(fullOnboardPath)
    val dfRenamed = df.toDF(taxonomyColumns: _*)
    val dfNull = dfRenamed.filter(col("customer").isNull)
    val dfNotNull = dfRenamed.filter(col("customer").isNotNull)
    
    val dfWithMiscColumns = dfNotNull.withColumn("party_id", getDataPartyId(col("customer"))).withColumn("file_path", input_file_name).withColumn("date_processed", current_timestamp)
    //val dfWithMiscColumns = dfRenamed.withColumn("file_path", input_file_name).withColumn("date_processed", current_timestamp)
    val dfWithDates = dfWithMiscColumns.withColumn("year", getYear(col("file_path"))).withColumn("month", getMonth(col("file_path"))).withColumn("day", getDay(col("file_path")))
   
    
    
     dfWithDates.write.partitionBy("year", "month", "day")
      .option("header", "true")
      .option("delimiter", "\t")
      .mode("overwrite")
      .csv(certFolderPath)
      
      dfNull.write.partitionBy("year", "month", "day")
      .option("header", "true")
      .option("delimiter", "\t")
      .mode("overwrite")
      .csv(failureFolderPath)
     
}
  
}