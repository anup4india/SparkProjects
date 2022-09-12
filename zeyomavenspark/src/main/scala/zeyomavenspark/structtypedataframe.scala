package zeyomavenspark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types._

object structtypedataframe {
  
  def main(args:Array[String]):Unit={
    
    val conf = new SparkConf().setAppName("StructType").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    
    val data = sc.textFile("file:///F:/Big Data/testfiles/txnsd.txt")
    val filterdata = data.map(x => x.split(","))
    val rowrdd = filterdata.map(x => Row(x(0),x(1),x(2))) //.filter(x => x(2).toString.contains("Gymnastics"))
    
    val spark = SparkSession.builder().getOrCreate()
    
    import spark.implicits._
    
    val schemastruct = new StructType()
                       .add("txnno", StringType)
                       .add("txndate", StringType)
                       .add("category", StringType)
                     
   val df = spark.createDataFrame(rowrdd, schemastruct)
   df.show()
   
   val simpleData = Seq(Row("James ","","Smith","36636","M",3000),
    Row("Michael ","Rose","","40288","M",4000),
    Row("Robert ","","Williams","42114","M",4000),
    Row("Maria ","Anne","Jones","39192","F",4000),
    Row("Jen","Mary","Brown","","F",-1))
    
  val simplerdd = spark.sparkContext.parallelize(simpleData) //sc.parallelize(simpleData)
  
  val simpleSchema = StructType(Array(
    StructField("firstname",StringType,true),
    StructField("middlename",StringType,true),
    StructField("lastname",StringType,true),
    StructField("id", StringType, true),
    StructField("gender", StringType, true),
    StructField("salary", IntegerType, true)
  ))
  
  val df1 = spark.createDataFrame(simplerdd, simpleSchema)
  //df1.printSchema()
  //df1.show()
  
  
  val structureData = Seq(
    Row(Row("James ","","Smith"),"36636","M",3100),
    Row(Row("Michael ","Rose",""),"40288","M",4300),
    Row(Row("Robert ","","Williams"),"42114","M",1400),
    Row(Row("Maria ","Anne","Jones"),"39192","F",5500),
    Row(Row("Jen","Mary","Brown"),"","F",-1)
  )
  

  val structureSchema = new StructType()
                        .add("name",new StructType()
                        .add("firstname",StringType)
                        .add("middlename",StringType)
                        .add("lastname",StringType))
                        .add("id",StringType)
                        .add("gender",StringType)
                        .add("salary",IntegerType)

  val df2 = spark.createDataFrame(spark.sparkContext.parallelize(structureData),structureSchema)
  df2.printSchema()
  df2.show()
    
  }
}