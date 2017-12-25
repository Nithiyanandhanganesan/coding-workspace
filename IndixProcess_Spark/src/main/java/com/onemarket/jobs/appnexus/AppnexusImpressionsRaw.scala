package com.onemarket.jobs.appnexus

import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.sql.functions.{col, concat_ws, current_timestamp, input_file_name, lit, udf}
import com.databricks.spark.avro._
import com.onemarket.utils.UserDefinedFunctions.getDataPartyId
import com.onemarket.utils.DateUtil.getDates
import org.joda.time.{DateTime, Period}
import scala.collection.mutable.WrappedArray
import scala.collection.Seq
import scala.reflect.api.materializeTypeTag

object AppnexusImpressionsRaw {
  val spark = SparkSession.builder.appName("SiteVisitsCertification").getOrCreate()

  import spark.implicits._

  // These config variables will be set through the config file
  var majorVersion = "1"
  var minorVersion = "0"
  var datasourceId = "10004"
  var srcPath = "gs://vendor-tdm-poc/raw/daily/appnexus_impressions"
  var certPath = "gs://dm-data-cert-poc-tier1/vendor-tdm/appnexus_impressions_raw"
  var failurePath = "gs://dm-data-failure-poc-tier1/vendor-tdm/appnexus_impressions_raw"
  var taxonomyColumns = Seq(
    "auction_id_64","date_time","user_tz_offset","creative_width","creative_height","media_type","fold_position","event_type",
"imp_type","payment_type","media_cost_cpm","revenue_type","media_cost","buyer_bid","ecp","eap","is_imp","is_learn","predict_type_rev",
"user_id_64","ip_address","ip_address_trunc","country","region","operating_system","browser","language","venue_id",
"seller_member_id","publisher_id","site_id","site_domain","tag_id","external_inv_id","reserve_price","seller_revenue_cpm",
"media_buy_rev_share_pct","pub_rule_id","seller_currency","publisher_currency","publisher_exchange_rate","serving_fees_cpm",
"serving_fees_revshare","buyer_member_id","advertiser_id","brand_id","advertiser_frequency","advertiser_recency","insertion_order_id",
"line_item_id","campaign_id","creative_id","creative_freq","creative_rec","cadence_modifier","can_convert","user_group_id",
"is_control","control_pct","control_creative_id","is_click","pixel_id","is_remarketing","post_click_conv",
"post_view_conv","post_click_revenue","post_view_revenue","order_id","external_data","pricing_type","booked_revenue","booked_revenue_adv_curr",
"commission_cpm","commission_revshare","auction_service_deduction","auction_service_fees","creative_overage_fees","clear_fees",
"buyer_currency","advertiser_currency","advertiser_exchange_rate","latitude","longitude","device_unique_id","device_id",
"carrier_id","deal_id","view_result","application_id","supply_type","sdk_version","ozone_id","billing_period_id","view_non_measurable_reason",
"external_uid","request_uuid","dma","city","mobile_app_instance_id","traffic_source_code","external_request_id","deal_type",
"ym_floor_id","ym_bias_id","is_filtered_request","age","gender","is_exclusive","bid_priority","custom_model_id",
"custom_model_last_modified","custom_model_leaf_name"
  )

  // Define miscellaneous functions here any function used in more than one class should go under the utils folder
  def countDelimiter = (delimiter: Char) => udf((str: String) => {
    str.count(_ == delimiter)
  })

  // Define Logic for config file
  def parseConfig() = {
    val configDf = spark.read.option("multiLine", "true").json("file://configs/site_visits_regular.json")

    majorVersion = configDf.select("process_properties.major_version").take(1)(0).getString(0)
    minorVersion = configDf.select("process_properties.minor_version").take(1)(0).getString(0)
    srcPath = configDf.select("process_properties.src_path").take(1)(0).getString(0)
    certPath = configDf.select("process_properties.cert_path").take(1)(0).getString(0)
    failurePath = configDf.select("process_properties.failure_path").take(1)(0).getString(0)
    taxonomyColumns = configDf.select($"process_properties.taxon_attribute").take(1)(0)(0).asInstanceOf[WrappedArray[String]].toSeq
  }

  // Define the error route
  def error(date: String) = {
    val fullSrcPath = s"$srcPath/$date/*/*/*.txt.gz"
    val fullFailurePath = s"$failurePath/$majorVersion/$minorVersion/$date/"

    // Drop records with incorrect number of delimiters
    val errorDf = spark.read.text(fullSrcPath)
    errorDf.withColumn("delimCount", countDelimiter('\t')($"value"))
      .filter($"delimCount" >= taxonomyColumns.length)
      .withColumn("file_path", input_file_name)
      .withColumn("date_processed", current_timestamp)
      .withColumn("error_message", lit("incorrect number of columns"))
      .drop($"delimCount")
      .write.mode("overwrite").json(fullFailurePath)

    // Drop records with null apn_user_id
    val df = spark.read.option("header", "false").option("mode", "DROPMALFORMED").option("delimiter", "\t").csv(fullSrcPath)
    val dfRenamed = df.toDF(taxonomyColumns: _*)

    val errorDfNull = dfRenamed.filter(col("user_id_64").isNull)
    errorDfNull.na.fill("").select(concat_ws("\t", errorDfNull.columns.map(col(_)): _*) as "value")
      .withColumn("file_path", input_file_name).withColumn("date_processed", current_timestamp)
      .withColumn("error_message", lit("user_id_64 is null"))
      .write.mode("append").json(fullFailurePath)
  }

  // Define the success route
  def success(date: String) = {
    
    val fullSrcPath = s"$srcPath/$date/*/*/*.txt.gz"
    val fullCertPath = s"$certPath/$majorVersion/$minorVersion/$date/"
    
    println(s"Reading data from :$fullSrcPath")

    val df = spark.read.option("header", "false").option("mode", "DROPMALFORMED").option("delimiter", "\t").csv(fullSrcPath)

    val dfRenamed = df.toDF(taxonomyColumns: _*)
    val dfWithMiscColumns = dfRenamed.withColumn("file_path", input_file_name).withColumn("date_processed", current_timestamp)
    val dfNotNull = dfWithMiscColumns.filter(col("user_id_64").isNotNull).withColumn("party_id", getDataPartyId(col("user_id_64")))
    dfNotNull.write.mode("overwrite").avro(fullCertPath)
    println(s"Successfully data loaded into : $fullCertPath");
  }

  def main(args: Array[String]) {
    // Manipulate dates to load the data you want
    //    parseConfig()
    getDates(DateTime.parse("2016-04-14"), DateTime.parse("2017-12-31"), Period.days(1)).foreach(date => {
      try {
        println(s"Starting Dataload process for $srcPath/$date/")
        error(date)
        success(date)
      } catch {
        case e: AnalysisException => { 
          e.printStackTrace()
          if (!e.getMessage().contains("Path does not exist")) 
               { throw e } 
          }
      }
    })

  }
}
