package zeyomavenspark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
//import org.apache.spark.implicits._

object rddschema {
  
  case class schema(txnno:String, category:String, product:String)
  
  def main(args : Array[String]):Unit ={
    val conf = new SparkConf().setAppName("schema").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    
    val data = sc.textFile("file:///F:/Big Data/testfiles/txnsmall.txt")
    val mapdata = data.map(x => x.split(","))
    val schemardd = mapdata.map(x => schema(x(0),x(1),x(2)))
    //schemardd.filter(x => x.category.contains("Gymnastics")).foreach(println)
    
    val spark = SparkSession
                .builder()
                .getOrCreate()
   import spark.implicits._
   val df = schemardd.toDF()
   df.show()
   df.createOrReplaceTempView("zeyodf")
   
   val df1 = spark.sql("select * from zeyodf where category = 'Gymnastics'")
   df1.show()
    
      
  }
}