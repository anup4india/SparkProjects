package zeyomavenspark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
/*import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
*/
object sparkstreaming {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("spark_performance").setMaster("local[*]")
      .set("spark.driver.allowMultipleContexts", "true")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().appName("spark").master("local[*]").getOrCreate()

    import spark.implicits._

  /*  val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "anup")*/

    val ssc = new StreamingContext(conf, Seconds(2))

    val data = ssc.textFileStream("file:///F:/Big Data/testfiles/data1.txt")

    data.print()

    ssc.start()
    ssc.awaitTermination()

  }
}