package zeyopack

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object dataframesfromdb {
  
  def main(args:Array[String]):Unit={
    
    val conf = new SparkConf().setAppName("dataframes").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    
    val spark = SparkSession.builder().getOrCreate()
    
    import spark.implicits._
    
    val sqldf = spark.read.format("jdbc")
                .option("driver", "com.mysql.jdbc.Driver")
                .option("url","jdbc:mysql://zeyodb.chwk6wgpdls3.ap-south-1.rds.amazonaws.com:3306/zeyodata")
                .option("dbtable","cashdata")
                .option("user","root")
                .option("password","Aditya908")
                .load()
                
    //sqldf.show()
    
    sqldf.createOrReplaceTempView("tempview")
    
    val filterdf = spark.sql("select * from tempview where category = 'Water Sports'")
    //filterdf.show()
/*    filterdf.write.format("jdbc")
                .option("driver", "com.mysql.jdbc.Driver")
                .option("url","jdbc:mysql://zeyodb.chwk6wgpdls3.ap-south-1.rds.amazonaws.com:3306/zeyodata")
                .option("dbtable","filterdata_anup")
                .option("user","root")
                .option("password","Aditya908")
                .mode("overwrite")
                .save()
*/    
   val sqlfilterdf = spark.read.format("jdbc")
                .option("driver", "com.mysql.jdbc.Driver")
                .option("url","jdbc:mysql://zeyodb.chwk6wgpdls3.ap-south-1.rds.amazonaws.com:3306/zeyodata")
                .option("dbtable","filterdata_anup")
                .option("user","root")
                .option("password","Aditya908")
                .load()
    //sqlfilterdf.show()
    
    //Read from XML file
    
    val xmldf = spark.read.format("com.databricks.spark.xml")
                      .option("rowTag", "book")
                      .load("file:///F:/Big Data/testfiles/book.xml")
                      
                      xmldf.show()
                      xmldf.printSchema()
    
  }
}