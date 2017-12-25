package com.onemarket.input
 
import org.apache.spark.sql.SparkSession

object IndixInputPrep {
  def main(args: Array[String]) {
    
    val spark =  SparkSession
    .builder().appName("IndixInputDataPrep")
    .master("local")
//    .enableHiveSupport()
    .getOrCreate()
   
//val input=spark.read.text("gs://dataproc-c83fd47e-0600-49f7-b8d0-58c9743354d5-us-west1/hadoop/tmp/bigquery/job_20171029005009_0000/shard-0/*.json")
    val input=spark.read.text("gs://polaris-dev/temp/12dm_test/site_visits-2017-06-01/sdc-000c942a-31e5-4d0a-99ad-9896c286fbf0")
//    val input=spark.read.text("file:////Users/nwe.nganesan/Desktop/junk/junk/*.txt") 
    val output=input.count()
     print("output is " + output)
     
     spark.stop()
  }
}
