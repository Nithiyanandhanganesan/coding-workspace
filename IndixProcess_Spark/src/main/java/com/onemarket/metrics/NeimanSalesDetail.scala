package com.onemarket.metrics

import java.nio.charset.StandardCharsets
import java.util.UUID
import com.samelamin.spark.bigquery._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, current_timestamp, input_file_name, udf}
import scala.collection.Seq
import scala.reflect.api.materializeTypeTag


object NeimanSalesDetail {


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
    val spark = SparkSession.builder.appName("Nordstrom Transactions").getOrCreate()

    val gcsOnboardPath = "vendor-tdm-poc/raw/daily/neiman_sales_detail"
    
    val fullOnboardPath = s"gs://$gcsOnboardPath/2016/*/*/*.gz"

    //val certFolderPath = s"gs://dm-data-cert-poc-tier1/vendor-tdm/nordstrom_transactions/"
    
    val certFolderPath = s"gs://dm-data-cert-poc-tier1/vendor-tdm/neiman_sales_detail_test/1/0"

    val taxonomyColumns = Seq("sales_audit_order_id","oms_order_id","product_id","web_item_id","web_item_name","brand_name","line_item_id","line_item_retail_sale",
        "line_item_promotional_sale","line_item_clearance_sale","line_item_retail_return_sale","line_item_promotional_return_sale",
        "line_item_clearance_return_sale","line_item_qty","line_item_status","department","class","subclass","oms_catalog","oms_item",
        "customer_order_date","custom_attribute_1","custom_attribute_2","custom_attribute_3","custom_attribute_4","custom_attribute_5",
        "custom_attribute_6","custom_attribute_7","custom_attribute_8","custom_attribute_10","custom_attribute_11",
        "custom_attribute_12","custom_attribute_13","custom_attribute_14","custom_attribute_15")

    val df = spark.read.option("header", "true").option("delimiter", "\t").csv(fullOnboardPath)
    val dfRenamed = df.toDF(taxonomyColumns: _*)
    //val dfWithMiscColumns = dfRenamed.withColumn("party_id", getDataPartyId(col("shopper_id"))).withColumn("file_path", input_file_name).withColumn("date_processed", current_timestamp)
    val dfWithMiscColumns = dfRenamed.withColumn("file_path", input_file_name).withColumn("date_processed", current_timestamp)
    val dfWithDates = dfWithMiscColumns.withColumn("year", getYear(col("file_path"))).withColumn("month", getMonth(col("file_path"))).withColumn("day", getDay(col("file_path")))
    
    val sourceFileCount=dfWithDates.select("file_path").distinct().count()
    
    var fileCount=0
    dfWithDates.createOrReplaceTempView("neiman_sales_detail_temp")
    val dfWithFileNameRowNum=spark.sql("select file_path,row_number () over (file_path) as row_num  from (select distinct file_path from neiman_sales_detail_temp")
    dfWithFileNameRowNum.createOrReplaceTempView("neiman_sales_detai_file_path")
    
    
    for (  fileCount <- 1 until  sourceFileCount.toInt)
    {
        val dfoutput=spark.sql(s"select * from neiman_sales_detail_temp where file_path in (select file_path from neiman_sales_detai_file_path where row_num= + $fileCount)")
         dfWithDates.write//.partitionBy("year", "month", "day")
        .option("header", "true")
        .option("delimiter", "\t")
        .mode("overwrite")
        .csv(certFolderPath)
        
    }
    
    dfWithDates.write.partitionBy("year", "month", "day")
      .option("header", "true")
      .option("delimiter", "\t")
      .mode("overwrite")
      .csv(certFolderPath)
  }
}
