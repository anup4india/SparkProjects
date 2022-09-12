package zeyomavenspark

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row

object dataframes {
  
  case class schema(txnno:String,txndate:String,custno:String,amount:String,category:String,product:String,city:String,state:String,spendby:String)
  
  
def main(args:Array[String]):Unit={
  
  val conf = new SparkConf().setAppName("dataframes").setMaster("local[*]")
  val sc = new SparkContext(conf)
  sc.setLogLevel("ERROR")
  
  val data = sc.textFile("file:///F:/Big Data/testfiles/txns")
  val mapdata = data.map(x => x.split(","))
  val schemadata = mapdata.map(x => schema(x(0),x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8)))
  val filterdata = schemadata.filter(x => x.spendby.contains("cash"))
  //filterdata.foreach(println)
  
  val spark = SparkSession.
              builder().
              getOrCreate()
  
  import spark.implicits._
              
  val df = filterdata.toDF()
  //df.show()
  
  }
}