package batch

import java.lang.management.ManagementFactory
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by Fedor.Hajdu on 12/22/2016.
  */
object BatchJob {
  def main(args: Array[String]) : Unit = {

    val conf = new SparkConf().setAppName("Lambda Architecture with Spark")

    // check if running from IDE
    if(ManagementFactory.getRuntimeMXBean.getInputArguments.toString.contains("IntelliJ IDEA")) {
      // download winutils for hadoop from here: https://github.com/srccodes/hadoop-common-2.2.0-bin/archive/master.zip
      System.setProperty("hadoop.home.dir", "D:\\HadoopUtils\\hadoop-common-2.2.0-bin-master\\bin")
      conf.setMaster("local[*]")
    }
  }
}
