package zeyopack
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import scala.io.Source
import org.apache.spark.sql.functions._

object fiillnullandjoins {
  
  def main(agrs:Array[String]):Unit={
    
    val conf = new SparkConf().setAppName("complexdata").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    
    val spark = SparkSession.builder().getOrCreate()
    
    import spark.implicits._
    
    val data = spark.read.option("delimiter",",").option("header","true").option("inferSchema","true").csv("file:///F:/Big Data/testfiles/nullfile.txt")
    data.show()
    data.printSchema()
    
    val nullint = data.na.fill(0)
    nullint.show()
    
    val nullstring = data.na.fill("")
    nullstring.show()
    
    val nullintfield = data.na.fill(0,Array("population"))
    val nullstringfield = data.na.fill("unknown", Array("type,state"))
                              .na.fill("",Array("city"))
    nullstringfield.show()
    nullintfield.show()
    
    
  }
  
}