package utils

import java.lang.management.ManagementFactory

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.{Duration, StreamingContext}

/**
  * Created by Fedor.Hajdu on 1/25/2017.
  */
object SparkUtils {
  val isIDE = {
    ManagementFactory.getRuntimeMXBean.getInputArguments.toString.contains("IntelliJ IDEA")
  }

  def getSparkContext(appName : String) = {
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

  def getStreamingContext(streamingApp : (SparkContext, Duration) => StreamingContext, sc: SparkContext, batchDuration: Duration) = {
    // try to recreate the context from an existing checkpoint if one exists
    val creatingFunc = () => streamingApp(sc, batchDuration)
    val ssc = sc.getCheckpointDir match {
      // createOnError will overwrite the checkpoint if the code differs from the one in the checkpoint (example, on new version)
      case Some(checkpointDir) => StreamingContext.getActiveOrCreate(checkpointDir, creatingFunc, sc.hadoopConfiguration, createOnError = true)
      case None => StreamingContext.getActiveOrCreate(creatingFunc)
    }

    // copy checkpoints from spark context to streaming context
    sc.getCheckpointDir.foreach(cp => ssc.checkpoint(cp))
    ssc
  }
}
