package zeyomavenspark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object dataframewrite {
  
  def main(args:Array[String]):Unit={
    
    val conf = new SparkConf().setAppName("dataframrwrite").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    
    val spark = SparkSession.builder().getOrCreate()
    
    val jsondata = spark.read.format("json").load("file:///F:/Big Data/testfiles/devices.json")
    //jsondata.show()
    
    val csvdata = spark.read.format("csv").option("header", "true").load("file:///F:/Big Data/testfiles/usdata.csv")
    csvdata.write.format("parquet").mode("overwrite").save("file:///F:/Big Data/testfiles/parquetdata")
    
    val parquetdata = spark.read.format("parquet").load("file:///F:/Big Data/testfiles/parquetdata")
    parquetdata.write.format("json").mode("overwrite").save("file:///F:/Big Data/testfiles/jsondata")
    
    val jsondata1 = spark.read.format("json").load("file:///F:/Big Data/testfiles/jsondata")
    jsondata1.write.format("orc").mode("overwrite").save("file:///F:/Big Data/testfiles/orcdata")
    
    val randomjson = spark.read.format("json").option("multiline", "true").load("file:///F:/Big Data/testfiles/random10.json")
    randomjson.show()
    
    randomjson.printSchema()
    
    
    
    //jsondata.write.format("parquet").save("file:///F:/Big Data/testfiles/parquetwritedata")
    //jsondata.write.format("csv").option("header", "true").option("delimiter", "~").mode("error").save("file:///F:/Big Data/testfiles/csvwritedata")
    //jsondata.write.format("csv").option("header", "true").option("delimiter", "~").mode("append").save("file:///F:/Big Data/testfiles/csvwritedata")
    //jsondata.write.format("csv").option("header", "true").option("delimiter", "~").mode("overwrite").save("file:///F:/Big Data/testfiles/csvwritedata")
    //jsondata.write.format("csv").option("header", "true").option("delimiter", "~").mode("ignore").save("file:///F:/Big Data/testfiles/csvwritedata")
    
  }
}