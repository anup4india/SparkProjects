package zeyopack

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object dataframesfromxml {
  
  def main(args:Array[String]):Unit={
    
    val conf = new SparkConf().setAppName("xmldataframe").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    
    val spark = SparkSession.builder().getOrCreate()
    
    val xmldf = spark.read.format("com.databricks.spark.xml").option("rowTag", "POSLog").load("file:///F:/Big Data/testfiles/transactions.xml")
    
    //xmldf.show()
    //xmldf.printSchema()
    
    val csvdf = spark.read.format("csv").option("header", "true").load("file:///F:/Big Data/testfiles/usdata.csv")
    //csvdf.show()
    //csvdf.filter("state='LA'").show()
    
    //csvdf.select("first_name", "last_name").show()
    
    csvdf.write.format("json").partitionBy("state","county").save("file:///F:/Big Data/testfiles/subpartition")
    
    
    
  }
}