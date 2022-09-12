package zeyomavenspark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object dataframes1 {
  
  case class schema(txnno:String,txndate:String,custno:String,amount:String,category:String,product:String,city:String,state:String,spendby:String)
  
  def main(args:Array[String]):Unit={
    
    val conf = new SparkConf().setAppName("dataframes").setMaster("local[*]")
    
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    
    val data = sc.textFile("file:///F:/Big Data/testfiles/txns")
    val mapdata = data.map(x => x.split(","))
    val schemadata = mapdata.map(x => schema(x(0),x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8)))
    
    val spark = SparkSession.builder().getOrCreate()
    
    import spark.implicits._
    
    val df = schemadata.toDF()
    
    df.createOrReplaceTempView("temp")
    
    val filterdata = spark.sql("select * from temp where spendby = 'cash'")
    filterdata.show()
    
    
    
  }
}