package zeyopack

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import scala.io.Source

object complexdatatasks {
  
  def main(args:Array[String]):Unit={
    
    val conf = new SparkConf().setAppName("complexdata").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    
    val data = spark.read.format("json").option("multiline", "true").load("file:///F:/Big Data/Complex Data/pets.json")
    
    data.show()
    data.printSchema()
    
    val flattendata = data.select(
        
        col("Name"),
        col("Mobile"),
        col("status"),
        col("Address.*"),
        explode(col("Pets").as("Pets"))
    )
    flattendata.show()
    flattendata.printSchema()
    
    val flattendata1 = data.withColumn("Permanent Address", expr("Address.`Permanent address`"))
                           .withColumn("Current Address", expr("Address.`current Address`"))
                           .withColumn("Pets", expr("explode(Pets)"))
                           .drop("Address")
    
    flattendata1.show()
    flattendata1.printSchema()
    
    val data2 = spark.read.format("json").option("multiline", "true").load("file:///F:/Big Data/Complex Data/donut.json")
    
    data2.show()
    data2.printSchema()
    
    val flattendata2 = data2.select(
        
        col("id"),
        col("name"),
        col("type"),
        col("image.height").as("iheight"),
        col("image.url").as("iurl"),
        col("image.width").as("iwidth"),
        col("thumbnail.height").as("theight"),
        col("thumbnail.url").as("turl"),
        col("thumbnail.width").as("twidth")
    )
    
    flattendata2.show()
    flattendata2.printSchema()
    
    val complexdata2 = flattendata2.select(
        
        col("id"),
        col("name"),
        col("type"),
        struct(
            col("iheight").as("height"),
            col("iurl").as("url"),
            col("iwidth").as("width")
            
        ).as("image"),
        struct(
            col("theight").as("height"),
            col("turl").as("url"),
            col("twidth").as("width")
            
        ).as("thumbnail")
    )
    
    complexdata2.show()
    complexdata2.printSchema()
    
    val urlfile = Source.fromURL("https://randomuser.me/api/0.8/?results=10").mkString
    
    val jsonfile = sc.parallelize(List(urlfile))
    
    val data3 = spark.read.json(jsonfile) 
    
        
    data3.show()
    data3.printSchema()
    
    val data4 = spark.read.format("json").option("multiline", "true").load("file:///F:/Big Data/Complex Data/zeyoc2.json")
    
    data4.show()
    data4.printSchema()
    
    
    val flattendata4 = data4.select(
        
        col("orgname"),
        struct(
            col("address.personaladdress"),
            col("otherdetails.name"),
            col("otherdetails.age")
        ).as("personel"),
        struct(
            col("address.professionaladdress"),
            col("otherdetails.designation"),
            col("otherdetails.role")
            
        ).as("profession")
    )
    
    flattendata4.show()
    flattendata4.printSchema()
    
  }
}