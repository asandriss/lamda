package utils

import java.lang.management.ManagementFactory

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

/**
  * Created by Fedor.Hajdu on 1/25/2017.
  */
object SparkUtils {
  def getSparkContext(appName : String) = {
    val isIDE = {
      ManagementFactory.getRuntimeMXBean.getInputArguments.toString.contains("IntelliJ IDEA")
    }
    var checkpointDirectory = ""

    val conf = new SparkConf().setAppName(appName)

    // check if running from IDE
    if(isIDE) {
      // download winutils for hadoop from here: https://github.com/srccodes/hadoop-common-2.2.0-bin/archive/master.zip
      System.setProperty("hadoop.home.dir", "D:\\HadoopUtils\\hadoop-common-2.2.0-bin-master")
      conf.setMaster("local[*]")
      checkpointDirectory = "file:///d:/tmp"
    } else {
      checkpointDirectory = "hdfs://lambda-pluralsight:9000/spark/checkpoint"
    }
    Logger.getLogger("org").setLevel(Level.OFF);
    Logger.getLogger("akka").setLevel(Level.OFF);

    // setup spark context
    val sc= SparkContext.getOrCreate(conf)
    sc.setCheckpointDir(checkpointDirectory)

    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)

    sc
  }

  def getSqlContext(sc : SparkContext) ={
    val sqlContext = SQLContext.getOrCreate(sc)
    sqlContext
  }
}
