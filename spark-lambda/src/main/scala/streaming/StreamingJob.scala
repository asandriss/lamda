package streaming
import org.apache.spark.streaming.{Seconds, StreamingContext}
import utils.SparkUtils._
/**
  * Created by Fedor.Hajdu on 1/25/2017.
  */
object StreamingJob {
  def main(args: Array[String]) : Unit = {
    val sc = getSparkContext("Streaming with Spark")

    val batchDuration = Seconds(4)
    val ssc = new StreamingContext(sc, batchDuration)

    val inputPath = isIDE match {
      case true => "file:///d:/boxes/spark-kafka-cassandra-applying-lambda-architecture/vagrant/input"
      case false => "file:///vagrant/input"
    }

    val textDStream = ssc.textFileStream(inputPath)
    textDStream.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
