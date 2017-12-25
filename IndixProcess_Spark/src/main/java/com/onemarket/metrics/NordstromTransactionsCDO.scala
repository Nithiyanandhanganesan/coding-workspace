package com.onemarket.metrics

import org.apache.spark.sql.{AnalysisException, DataFrame, DataFrameReader, SparkSession}
import org.joda.time.format.DateTimeFormat
import org.joda.time.{DateTime, Period}
import play.api.libs.json.Json
import com.samelamin.spark.bigquery._


object NordstromTransactionsCDO {

  def getCount(dfr: DataFrameReader, path: String): Long = {
    try {
      dfr.csv(path).count()
    } catch {
      case e: AnalysisException => {
        e.printStackTrace()
        0
      }
    }
  }

  def main(args: Array[String]) {

    def dateRange(from: DateTime, to: DateTime, step: Period): Iterator[DateTime] =
      Iterator.iterate(from)(_.plus(step)).takeWhile(!_.isAfter(to))

    val jsonConfig =
      """
        {
          "load_type": "batch",
          "load_frquency": "Daily",
          "datasource_id": 10004,
          "party_owner": 26111,
          "ds_entity": "nordstrom_transactions",
          "retailer_id": 26111,
          "retailer_name": "Nordstrom",
          "metric_key_field": "NA",
          "metric_key_field_value":"NA",
          "onboard_folder_path":"gs://vendor-tdm-poc/raw/daily/nordstrom_transactions",
          "cert_folder_path":"gs://dm-data-cert-poc-tier1/vendor-tdm/nordstrom_transactions/1/0",
          "failure_folder_path":"gs://dm-data-failure-poc-tier1/vendor-tdm/nordstrom_transactions/1/0",
          "bq_table_name":"om_cert_nordstrom.nordstrom_transactions",
          "bq_table_file_path":"gs://vendor-tdm-poc/raw/daily/nordstrom_transactions"
        }
      """

    val spark = SparkSession.builder.appName("Nordstrom Transactions").getOrCreate()
    spark.sqlContext.setBigQueryProjectId("poc-tier1")
    spark.sqlContext.setBigQueryGcsBucket("testbucketdeletemeuseless")
    spark.sqlContext.useStandardSQLDialect(true)

    val fmt = DateTimeFormat.forPattern("yyyy/MM/dd")
    val range = dateRange(
      DateTime.parse("2016-01-01"),
      DateTime.parse("2016-12-31"),
      Period.days(1)).toList.map(x => fmt.print(x))

    val json = Json.parse(jsonConfig)

//    val dateString = args(0)
    range.foreach(dateString => {
      val loadType = json("load_type").as[String]
      val loadFrequency = json("load_frquency").as[String]
      val datasourceId = json("datasource_id").as[Int]
      val partyOwner = json("party_owner").as[Int]
      val dsEntity = json("ds_entity").as[String]
      val retailerId = json("retailer_id").as[Int]
      val retailerName = json("retailer_name").as[String]
      val metricKeyField = json("metric_key_field").as[String]
      val metricKeyFieldValue = json("metric_key_field_value").as[String]
      val onboardFolder = s"${json("onboard_folder_path").as[String]}/$dateString"
      val certFolder = s"${json("cert_folder_path").as[String]}/$dateString"
      val failureFolder = s"${json("failure_folder_path").as[String]}/$dateString"
      val bqTableName = json("bq_table_name").as[String]
      val bqTableFilePath = s"${json("bq_table_file_path").as[String]}/$dateString"
      val certSales = 0
      val failureSales = 0
      val loadStatus = "SUCCESS"
      val metricStatus = "SUCCESS"
      val lastRunDate = fmt.print(DateTime.now())

      val usaDateFormat = DateTimeFormat.forPattern("MM/dd/yyyy")
        .print(DateTime.parse(dateString, DateTimeFormat.forPattern("yyyy/MM/dd")))
      println(usaDateFormat)

      val onboardRecordCount = getCount(spark.read.option("header", "false").option("delimiter", "\u00FE"), onboardFolder)
      val certRecordCount = getCount(spark.read.option("header", "true").option("delimiter", "\u00FE"), certFolder)
      val failureRecordCount = getCount(spark.read.option("header", "true").option("delimiter", "\u00FE"), failureFolder)
      val bqCount = spark.sqlContext.bigQuerySelect(s"""
          SELECT *
          FROM `poc-tier1.om_cert_nordstrom.nordstrom_transactions`
          WHERE event_time LIKE "%$usaDateFormat%"
        """).count()
      println(onboardRecordCount)
      println(certRecordCount)
      println(failureRecordCount)
      println(bqCount)
      println(s"""
           INSERT INTO om_onboard_metrics.zee (
             load_type, load_frquency, datasource_id, party_owner, ds_entity,
             retailer_id, retailer_name, metric_key_field, metric_key_field_value,
             onboard_folder_path, cert_folder_path, failure_folder_path, bq_table_name,
             bq_table_file_path, onboard_record_count, cert_record_count, failure_record_count,
             bq_record_count, cert_sales, failure_sales, load_status, metrics_status, last_run_date, feed_date
          ) VALUES (
             "$loadType", "$loadFrequency", "$datasourceId", "$partyOwner", "$dsEntity",
             "$retailerId", "$retailerName", "$metricKeyField", "$metricKeyFieldValue",
             "$onboardFolder", "$certFolder", "$failureFolder", "$bqTableName",
             "$bqTableFilePath", "$onboardRecordCount", "$certRecordCount", "$failureRecordCount",
             "$bqCount", NULL, NULL, "$loadStatus", "$metricStatus", TO_DATE("$lastRunDate", "YYYY/MM/DD"), TO_DATE("$dateString", "YYYY/MM/DD"))
        """)
      spark.sqlContext.runDMLQuery(
        s"""
           INSERT INTO om_onboard_metrics.zee (
             load_type, load_frquency, datasource_id, party_owner, ds_entity,
             retailer_id, retailer_name, metric_key_field, metric_key_field_value,
             onboard_folder_path, cert_folder_path, failure_folder_path, bq_table_name,
             bq_table_file_path, onboard_record_count, cert_record_count, failure_record_count,
             bq_record_count, cert_sales, failure_sales, load_status, metrics_status, last_run_date, feed_date
          ) VALUES (
             "$loadType", "$loadFrequency", $datasourceId, $partyOwner, "$dsEntity",
             $retailerId, "$retailerName", "$metricKeyField", "$metricKeyFieldValue",
             "$onboardFolder", "$certFolder", "$failureFolder", "$bqTableName",
             "$bqTableFilePath", $onboardRecordCount, $certRecordCount, $failureRecordCount,
             $bqCount, NULL, NULL, "$loadStatus", "$metricStatus", PARSE_DATE("%Y/%m/%d", "$lastRunDate"), PARSE_DATE("%Y/%m/%d", "$dateString"))
        """)
    })
  }
}
