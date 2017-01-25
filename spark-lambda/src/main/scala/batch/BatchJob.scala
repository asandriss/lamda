package batch

import java.lang.management.ManagementFactory

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.{Level, Logger}
import domain._
import utils.SparkUtils._
import org.apache.spark.sql.{SQLContext, SaveMode}
/**
  * Created by Fedor.Hajdu on 12/22/2016.
  */
object BatchJob {
  def main(args: Array[String]) : Unit = {

    val sparkContext = getSparkContext("Lambda Architecture with Spark")
    val sqlContext = getSqlContext(sparkContext)

    import org.apache.spark.sql.functions._
    import sqlContext.implicits._

    // initialize RDD
//    val sourceFile = "D:\\boxes\\spark-kafka-cassandra-applying-lambda-architecture\\vagrant\\data.tsv"
    val sourceFile = "file:///vagrant/data.tsv"   // use path YARN can read - vagrant is mounted directly.
    val input = sparkContext.textFile(sourceFile)

    // Change RDD to data frame
    val inputDF = input.flatMap { line =>
      val record = line.split("\\t")
      val ms_in_hour = 1000*60*60
      if(record.length == 7)
        Some(Activity(record(0).toLong / ms_in_hour * ms_in_hour, record(1), record(2), record(3), record(4), record(5), record(6)))
      else
        None
    }.toDF()

    val df = inputDF.select(
      add_months(from_unixtime(inputDF("timestamp_hour")/1000), 1).as("timestamp_hour"),
      inputDF("referrer"), inputDF("action"), inputDF("prevPage"), inputDF("page"), inputDF("visitor"),inputDF("product")
    ).cache()

    df.registerTempTable("activity")

    val visitorsByProduct = sqlContext.sql(
      """SELECT product, timestamp_hour, COUNT(DISTINCT visitor) as unique_visitors
        |FROM activity
        |GROUP BY product, timestamp_hour
      """.stripMargin)
    val activityByProduct = sqlContext.sql(
      """SELECT
        |product,
        |timestamp_hour,
        |sum(case when action = 'purchase' then 1 else 0 end) as purchase_count,
        |sum(case when action = 'add_to_cart' then 1 else 0 end) as add_to_cart_count,
        |sum(case when action = 'page_view' then 1 else 0 end) as page_view_count
        |FROM activity
        |group by product, timestamp_hour
      """.stripMargin).cache()

    activityByProduct.write
      .partitionBy("timestamp_hour")      // this will create subdirectory with timestamp value
      .mode(SaveMode.Append)
      .parquet("hdfs://lambda-pluralsight:9000/lambda/batch1")

    visitorsByProduct.foreach(println)
    println("****** Activity by product************")
    activityByProduct.foreach(println)
  }
}
