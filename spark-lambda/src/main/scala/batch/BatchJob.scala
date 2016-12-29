package batch

import java.lang.management.ManagementFactory
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.log4j.{Level, Logger}
import domain._
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

    // setup spark context
    val sc= new SparkContext(conf)
    val rootLogger = Logger.getRootLogger()
    Logger.getLogger("org").setLevel(Level.OFF);
    Logger.getLogger("akka").setLevel(Level.OFF);
    rootLogger.setLevel(Level.ERROR)

    // initialize RDD
    val sourceFile = "file:///d:/tmp/spark-test/data.tsv"
    val input = sc.textFile(sourceFile)
    val inputRdd = input.flatMap { line =>
      val record = line.split("\\t")
      val ms_in_hour = 1000*60*60
      if(record.length == 7)
        Some(Activity(record(0).toLong / ms_in_hour, record(1), record(2), record(3), record(4), record(5), record(6)))
      else
        None
    }

    val keyedByProduct = inputRdd.keyBy(a => (a.product, a.timestamp_hour)).cache()
    val visitorsByProduct = keyedByProduct
      .mapValues(p => p.visitor)
      .distinct()
      .countByKey()     // helper for reduce action

    val activityByProduct = keyedByProduct    // will use cached version now
        .mapValues { p => // create a map of actions
          p.action match {
            case "purchase" => (1, 0, 0)
            case "add_to_cart" => (0, 1, 0)
            case "page_view" => (0, 0, 1)
          }
        }
        //.reduceByKey( (a, b) => (a._1 + b._1,a._2 + b._2,a._3 + b._3)) // reduce action sum the bits to get actual counts

    println("Visitors By Product:")
    visitorsByProduct.foreach(println)

    println("*****************************************************************************************")
    println("Activity by Product:")
    activityByProduct.foreach(println)
  }
}
