package zeyopack

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object complexdata {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("complexdata").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().getOrCreate()

    val data = spark.read.format("json").option("multiline", "true").load("file:///F:/Big Data/Complex Data/donut.json")

    data.show()
    data.printSchema()

    val flattendata = data.select(
      col("id"),
      col("image.height").as("iheight"),
      col("image.url").as("iurl"),
      col("image.width").as("iwidth"),
      col("name"),
      col("thumbnail.height").as("theight"),
      col("thumbnail.url").as("turl"),
      col("thumbnail.width").as("twidth"),
      col("type"))
    flattendata.show()
    flattendata.printSchema()

    val data1 = spark.read.format("json").option("multiline", "true").load("file:///F:/Big Data/Complex Data/zeyoc.json")

    data1.show()
    data1.printSchema()

    val flattendata1 = data1.select(
      "trainer",
      "orgname",
      "address.*"
    //"address.permanentAddress",
    //"address.temporaryAddress"
    )

    flattendata1.show()
    flattendata1.printSchema()

    val data2 = spark.read.format("json").option("multiline", "true").load("file:///F:/Big Data/Complex Data/place.json")

    data2.show()
    data2.printSchema()

    val flattendata2 = data2.select(
      "place",
      "user.name",
      "user.address.*")
    flattendata2.show()
    flattendata2.printSchema()

    val data3 = spark.read.format("json").option("multiline", "true").load("file:///F:/Big Data/Complex Data/topping.json")
    data3.show()
    data3.printSchema()

    val flattendata3 = data3.select(
      col("batters.batter.id").as("bid"),
      col("batters.batter.type").as("btype"),
      col("id"),
      col("name"),
      col("ppu"),
      col("topping.id").as("tid"),
      col("topping.type").as("ttype"),
      col("type"))
    
    flattendata3.show()
    flattendata3.printSchema()
    
    val complexdata3 = flattendata3.select(
        
        col("id"),
        col("name"),
        col("ppu"),
        col("type"),
        struct(
            struct(
                col("bid").as("id"),
                col("btype").as("type")
                
            ).as("batter")
            
        ).as("batters"),
        struct(
            col("tid").as("id"),
            col("ttype").as("type")
            
        ).as("topping")
    )
    
    complexdata3.show()
    complexdata3.printSchema()

  }
}