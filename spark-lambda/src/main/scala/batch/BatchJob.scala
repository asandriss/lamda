package batch

import java.lang.management.ManagementFactory

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.{Level, Logger}
import domain._
import org.apache.spark.sql.SQLContext
/**
  * Created by Fedor.Hajdu on 12/22/2016.
  */
object BatchJob {
  def main(args: Array[String]) : Unit = {

    val conf = new SparkConf()
      .setAppName("Lambda Architecture with Spark")

    // check if running from IDE
    if(ManagementFactory.getRuntimeMXBean.getInputArguments.toString.contains("IntelliJ IDEA")) {
      // download winutils for hadoop from here: https://github.com/srccodes/hadoop-common-2.2.0-bin/archive/master.zip
      System.setProperty("hadoop.home.dir", "D:\\HadoopUtils\\hadoop-common-2.2.0-bin-master")
      conf.setMaster("local[*]")
    }
    Logger.getLogger("org").setLevel(Level.OFF);
    Logger.getLogger("akka").setLevel(Level.OFF);

    // setup spark context
    val sc= new SparkContext(conf)
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)

    implicit val sqlContext = new SQLContext(sc)

    import org.apache.spark.sql.functions._
    import sqlContext.implicits._

    // initialize RDD
    val sourceFile = "file:///d:/tmp/spark-test/data.tsv"
    val input = sc.textFile(sourceFile)

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
      //add_months(inputDF("timestamp_hour"), 1).as("timestamp_hour"),
      inputDF("timestamp_hour"),
      inputDF("referrer"), inputDF("action"), inputDF("prevPage"), inputDF("page"), inputDF("visitor"),inputDF("product")
    ).cache()

    df.registerTempTable("activity")

    val visitorsByProduct = sqlContext.sql(
      """SELECT product, timestamp_hour, COUNT(DISTINCT visitor) as unique_visitors
        |FROM activity
        |GROUP BY product, timestamp_hour
      """.stripMargin)

    visitorsByProduct.printSchema()
    visitorsByProduct.foreach(println)
  }
}
