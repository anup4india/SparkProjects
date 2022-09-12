package zeyopack

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object complexdatacreation {
  
  def main(args:Array[String]):Unit={
    
    val conf = new SparkConf().setAppName("complexdata").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    
    val data = spark.read.format("json").option("multiline", "true").load("file:///F:/Big Data/Complex Data/zeyo.json")
    
    //data.show()
    //data.printSchema()
    
    val flattendata = data.select(
    
        col("orgname"),
        col("trainer"),
        col("address.permanentAddress").as("permanentAddress"),
        col("address.temporaryAddress").as("temporaryAddress")
    )
    
    //flattendata.show()
    //flattendata.printSchema()
    
    val complexdata = flattendata.select(
    
        col("orgname"),
        col("trainer"),
        struct(
            col("permanentAddress"),
            col("temporaryAddress")
            
        ).as("address")
    
    )
    //complexdata.show()
    //complexdata.printSchema()
    
    val data1 = spark.read.format("json").option("multiline", "true").load("file:///F:/Big Data/Complex Data/zeyoc.json")
    data1.show()
    data1.printSchema()
    
    val flattendata1 = data1.select(
    
        col("orgname"),
        col("trainer"),
        col("address.permanentAddress.area").as("parea"),
        col("address.permanentAddress.location").as("plocation"),
        col("address.temporaryAddress.area").as("tarea"),
        col("address.temporaryAddress.location").as("tlocation")
    
    )
    
    flattendata1.show()
    flattendata1.printSchema()
    
    val complexdata1 = flattendata1.select(
    
        col("orgname"),
        col("trainer"),
        struct(
            
            struct(
                
                col("parea").as("area"),
                col("plocation").as("location")
                
            ).as("permanentAddress"),
            struct(
                col("tarea").as("area"),
                col("tlocation").as("location")
            
            ).as("temporaryAddress")
            
        
        ).as("address")
        
    )
    
    complexdata1.show()
    complexdata1.printSchema()
    
  }
}