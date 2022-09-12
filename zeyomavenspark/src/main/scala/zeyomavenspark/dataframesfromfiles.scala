package zeyomavenspark

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object dataframesfromfiles {

  def main(args: Array[String]): Unit = {
    
    val conf = new SparkConf().setAppName("dataframes").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession
      .builder().getOrCreate()

      // CSV file
    val csvdata = spark.read.format("csv").option("header", "true").load("file:///F:/Big Data/testfiles/usdata.csv")
    //data.show()
    
    csvdata.createOrReplaceTempView("usdata")
    
    val filterdata = spark.sql("select * from usdata where state = 'OH'")
    
    //filterdata.show()
    
    // Paraquet file
    val pardata = spark.read.format("parquet").load("file:///F:/Big Data/testfiles/part_par.parquet")
    //pardata.show()
    
    pardata.createOrReplaceTempView("pardata")
    //spark.sql("select * from pardata where state = 'OH'").show()
    
    //JSON file
    val jsondata = spark.read.format("json").load("file:///F:/Big Data/testfiles/devices.json")
    //jsondata.show()
    
    jsondata.createOrReplaceTempView("jsondata")
    
    //spark.sql("select * from jsondata where temp > 30").show()
    
    // ORC file
    val orcdata = spark.read.format("orc").load("file:///F:/Big Data/testfiles/orcdata.orc")
    //orcdata.show()
    
    orcdata.createOrReplaceTempView("orcdata")
    //spark.sql("select * from orcdata where age < 30").show()
    
    // Avro File
    val avrodata = spark.read.format("avro").load("file:///F:/Big Data/testfiles/part_av.avro")
    avrodata.show()
    
    

  }
}