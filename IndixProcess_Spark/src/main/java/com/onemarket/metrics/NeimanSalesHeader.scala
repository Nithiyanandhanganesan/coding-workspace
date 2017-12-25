package com.onemarket.metrics

import java.nio.charset.StandardCharsets
import java.util.UUID
import com.samelamin.spark.bigquery._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, current_timestamp, input_file_name, udf}
import scala.collection.Seq
import scala.reflect.api.materializeTypeTag
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path

import java.text.SimpleDateFormat
import java.util.Date
import java.util.Calendar
import java.time.LocalDate
import scala.collection.mutable.MutableList

object NeimanSalesHeader {
  def getDataPartyId = udf((key: String) => {
      UUID.nameUUIDFromBytes(key.toLowerCase.getBytes(StandardCharsets.ISO_8859_1)).toString  
  })


  def getYear = udf((x: String) => {
     x.split("/").takeRight(4).slice(0,1).toString()
  })

  def getMonth = udf((x: String) => {
     x.split("/").takeRight(3).slice(0,1).toString()
  })

  def getDay = udf((x: String) => {
     x.split("/").takeRight(2).slice(0,1).toString()
  })
  
  

  def main(args: Array[String]) {
    val spark = SparkSession.builder.appName("Neiman Sales Header").getOrCreate()
    
    val taxonomy=spark.read.option("multiLine","true").json("config/neiman.json")
    taxonomy.cache()
    
    val gcsOnboardPath=taxonomy.select("process_properties.gcs_src_path").collect.map(_.getString(0)).mkString(" ")
    val certFolderPath=taxonomy.select("process_properties.gcs_cert_path").collect.map(_.getString(0)).mkString(" ")
    val failureFolderPath=taxonomy.select("process_properties.gcs_failure_path").collect.map(_.getString(0)).mkString(" ")
    
   
    //val gcsOnboardPath = "gs://vendor-tdm-poc/raw/daily/neiman_sales_header"
    //val certFolderPath = "gs://dm-data-cert-poc-tier1/vendor-tdm/neiman_sales_header1/1/0"
    //val failureFolderPath = "gs://dm-data-failure-poc-tier1/vendor-tdm/neiman_sales_header1/1/0"    
    //val fullOnboardPath = s"gs://$gcsOnboardPath/2016/09/14/*.gz"
    
    
    val start="2017-07-23"
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd");
    var startDate = dateFormat.parse(start);
    val current = new SimpleDateFormat("yyyy-MM-dd").format(new Date());
    val endDate = dateFormat.parse(current)
    val c = Calendar.getInstance()
    
    while ( startDate.before(endDate) || startDate.equals(endDate)) {
      c.setTime(startDate)
      c.add(Calendar.DATE, 1)
      startDate = c.getTime
      startDate.formatted("yyyy-MM-dd")
      val loadYear = new SimpleDateFormat("yyyy").format(startDate) 
      val loadMonth = new SimpleDateFormat("MM").format(startDate) 
      val loadDate = new SimpleDateFormat("dd").format(startDate) 
      
      val taxonomyColumns = Seq("sold_customer","shipping_customer","partner_channel","partner_site_id","sales_audit_order_id","store_order_id","oms_order_id","web_order_id","store_id","transaction_date","transaction_type","return_flag","register_id","associate_id","original_order_id","original_associate_id",
        "source_channel","gross_sales","total_returns","total_discounts","net_sales","sold_units","return_units","net_units","transaction_audit_flag","order_type",
        "customer_order_date","custom_attribute_1","custom_attribute_2","custom_attribute_3","custom_attribute_4","custom_attribute_5","custom_attribute_6",
        "custom_attribute_7","custom_attribute_8","custom_attribute_9","custom_attribute_10","custom_attribute_11","custom_attribute_12",
        "custom_attribute_13","custom_attribute_14","custom_attribute_15")
      
      println(s"Data laod for: ${gcsOnboardPath}/${loadYear}/${loadMonth}/${loadDate}")
      println(s"Target data load path: ${certFolderPath}/${loadYear}/${loadMonth}/${loadDate}")
      val path = new Path(s"${gcsOnboardPath}/${loadYear}/${loadMonth}/${loadDate}");
      val fs=path.getFileSystem(spark.sparkContext.hadoopConfiguration)
      
      if ((fs.exists(path)) == true)
      {
      
      val df = spark.read.option("header", "true").option("delimiter", "\t").csv(s"${gcsOnboardPath}/${loadYear}/${loadMonth}/${loadDate}/*.gz")
      val dfRenamed = df.toDF(taxonomyColumns: _*)
      val dfNull = dfRenamed.filter(col("shipping_customer").isNull)
      val dfNotNull = dfRenamed.filter(col("shipping_customer").isNotNull)
      
      val dfWithMiscColumns = dfNotNull.withColumn("party_id", getDataPartyId(col("shipping_customer"))).withColumn("file_path", input_file_name).withColumn("date_processed", current_timestamp)
    //val dfWithMiscColumns = dfRenamed.withColumn("file_path", input_file_name).withColumn("date_processed", current_timestamp)
    //  val dfWithDates = dfWithMiscColumns.withColumn("year", getYear(col("file_path"))).withColumn("month", getMonth(col("file_path"))).withColumn("day", getDay(col("file_path")))
   
    
      dfWithMiscColumns.write.option("header", "true").option("delimiter", "\t").mode("overwrite").csv(s"${certFolderPath}/${loadYear}/${loadMonth}/${loadDate}")
      dfNull.write.option("header", "true").option("delimiter", "\t").mode("overwrite").csv(s"${failureFolderPath}/${loadYear}/${loadMonth}/${loadDate}")
      println(s"Successfully Data loaded into: ${certFolderPath}/${loadYear}/${loadMonth}/${loadDate}")
      }
      else
      {
         println(s"Folder does not exists: ${gcsOnboardPath}/${loadYear}/${loadMonth}/${loadDate}")
         println("Skipping...")
      }
      }
    
    

    //val certFolderPath = s"gs://dm-data-cert-poc-tier1/vendor-tdm/nordstrom_transactions/"
    
/*    val certFolderPath = s"gs://dm-data-cert-poc-tier1/vendor-tdm/neiman_sales_header/1/0"

    val taxonomyColumns = Seq("sold_customer","shipping_customer","partner_channel","partner_site_id","sales_audit_order_id","store_order_id","oms_order_id","web_order_id","store_id","transaction_date","transaction_type","return_flag","register_id","associate_id","original_order_id","original_associate_id",
        "source_channel","gross_sales","total_returns","total_discounts","net_sales","sold_units","return_units","net_units","transaction_audit_flag","order_type",
        "customer_order_date","custom_attribute_1","custom_attribute_2","custom_attribute_3","custom_attribute_4","custom_attribute_5","custom_attribute_6",
        "custom_attribute_7","custom_attribute_8","custom_attribute_9","custom_attribute_10","custom_attribute_11","custom_attribute_12",
        "custom_attribute_13","custom_attribute_14","custom_attribute_15")


    val df = spark.read.option("header", "true").option("delimiter", "\t").csv(fullOnboardPath)
    val dfRenamed = df.toDF(taxonomyColumns: _*)
    val dfNull = dfRenamed.filter(col("shipping_customer").isNull)
    val dfNotNull = dfRenamed.filter(col("shipping_customer").isNotNull)
    
    val dfWithMiscColumns = dfNotNull.withColumn("party_id", getDataPartyId(col("shipping_customer"))).withColumn("file_path", input_file_name).withColumn("date_processed", current_timestamp)
    //val dfWithMiscColumns = dfRenamed.withColumn("file_path", input_file_name).withColumn("date_processed", current_timestamp)
    val dfWithDates = dfWithMiscColumns.withColumn("year", getYear(col("file_path"))).withColumn("month", getMonth(col("file_path"))).withColumn("day", getDay(col("file_path")))
   
    
    
     dfWithDates.write.partitionBy("year", "month", "day")
      .option("header", "true")
      .option("delimiter", "\t")
      .mode("overwrite")
      .csv(certFolderPath)*/
}
  
}
  