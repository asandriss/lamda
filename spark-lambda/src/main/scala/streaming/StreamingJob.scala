package streaming
import org.apache.spark.SparkContext
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}
import utils.SparkUtils._
/**
  * Created by Fedor.Hajdu on 1/25/2017.
  */
object StreamingJob {
  def main(args: Array[String]) : Unit = {
    val sc = getSparkContext("Streaming with Spark")

    val batchDuration = Seconds(4)

    def streamingApp(sc:SparkContext, batchDuration: Duration) = {
      val ssc = new StreamingContext(sc, batchDuration)

      val inputPath = isIDE match {
        case true => "file:///d:/boxes/spark-kafka-cassandra-applying-lambda-architecture/vagrant/input"
        case false => "file:///vagrant/input"
      }

      val textDStream = ssc.textFileStream(inputPath)
      textDStream.print()
      ssc
    }

    val ssc = getStreamingContext(streamingApp, sc, batchDuration)
    ssc.start()
    ssc.awaitTermination()
  }
}
