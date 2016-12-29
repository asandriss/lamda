package batch

import java.lang.management.ManagementFactory
import org.apache.spark.{SparkContext, SparkConf}
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
      System.setProperty("hadoop.home.dir", "D:\\HadoopUtils\\hadoop-common-2.2.0-bin-master\\bin")
      conf.setMaster("local[*]")
    }

    // setup spark context
    val sc= new SparkContext(conf)

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
  }
}
