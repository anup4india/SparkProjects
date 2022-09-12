package project

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import scala.io.Source
import org.apache.spark.sql.functions._

object sparkproject extends App {

  val conf = new SparkConf().setAppName("SparkProject").setMaster("local[*]")
  val sc = new SparkContext(conf)
  sc.setLogLevel("ERROR")

  val spark = SparkSession.builder().getOrCreate()

  import spark.implicits._

  val avrodata = spark.read.format("avro").load("file:///F:/Big Data/SparkProject/projectsample.avro")
  println("avro data")
  //avrodata.show()

  val urlstring = Source.fromURL("https://randomuser.me/api/0.8/?results=1000").mkString

  val urldatardd = sc.parallelize(List(urlstring))

  val urldatadf = spark.read.json(urldatardd)
  //urldatadf.printSchema()
  //urldatadf.withColumn("results", expr("explode(results)")).printSchema()

  val flattendatadf = urldatadf.withColumn("results", expr("explode(results)"))
    .select(

      "nationality",
      "seed",
      "version",
      "results.user.*",
      "results.user.location.*",
      "results.user.name.*",
      "results.user.picture.*").drop("location", "name", "picture")

  //flattendatadf.printSchema()
  //flattendatadf.show()

  //val filterdata = flattendatadf.withColumn("username", regexp_replace($"username", "[0-9]", ""))
  val filterdata = flattendatadf.withColumn("username", expr("regexp_replace(username, '[0-9]', '')"))
  //filterdata.show()

  val joineddata = avrodata.join(broadcast(filterdata), Seq("username"), "left")
  //joineddata.show()
  //joineddata.printSchema()
  
  val availablecust = joineddata.filter(col("nationality").isNotNull)
  val unavailablecust = joineddata.filter(col("nationality").isNull)
  //availablecust.show()
  //unavailablecust.show()
  
  val notavailable = unavailablecust.na.fill("NotAvailable").na.fill(0)
  //notavailable.show()
  
  availablecust.withColumn("currentdate", current_date)show()
  notavailable.withColumn("currentdate", current_date).show()
  println(urldatadf.rdd.getNumPartitions)
 
}