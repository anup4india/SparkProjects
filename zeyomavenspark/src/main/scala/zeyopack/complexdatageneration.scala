package zeyopack

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import scala.io.Source
import org.apache.spark.sql.functions._

object complexdatageneration {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("complexdata").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    val data = spark.read.format("json").option("multiline", "true").load("file:///F:/Big Data/Complex Data/zeyoc.json")

    //data.show()
    //data.printSchema()

    val flattendata = data.withColumn("Students", expr("explode(Students)"))

    //flattendata.show()
    //flattendata.printSchema()

    val complexdata = flattendata.groupBy("orgname", "trainer", "address").agg(collect_list("Students").as("Students"))
    //complexdata.show()
    //complexdata.printSchema()

    val data1 = spark.read.format("json").option("multiline", "true").load("file:///F:/Big Data/Complex Data/users.json")

    //data1.show()
    //data1.printSchema()

    val flattendata1 = data1.withColumn("users", expr("explode(users)"))
      .select(

        col("page"),
        col("per_page"),
        col("total"),
        col("total_pages"),
        col("users.*"))

    //flattendata1.show()
    //flattendata1.printSchema()

    val complexdata1 = flattendata1.groupBy("page", "per_page", "total", "total_pages")
      .agg(collect_list(

        struct(
          col("emailAddress"),
          col("firstName"),
          col("lastName"),
          col("phoneNumber"),
          col("userId"))).as("users"))

    //complexdata1.show()
    //complexdata1.printSchema()
    
    val data2 = spark.read.format("json").option("multiline", "true").load("file:///F:/Big Data/Complex Data/zeyoc4.json")
    //data2.show()
    //data2.printSchema()
    
    val flattendata2 = data2.withColumn("Students", expr("explode(Students)"))
                            .withColumn("tools", expr("explode(Students.tools)"))
                            .select(
                                
                                col("Students.age"),
                                col("Students.name"),
                                col("orgname"),
                                col("trainer"),
                                col("tools")
                                
                            
                            )
    
    //flattendata2.show()
    //flattendata2.printSchema()
    
    val data3 = spark.read.format("json").option("multiline", "true").load("file:///F:/Big Data/Complex Data/users.json")
    
    data3.show()
    data3.printSchema()
    
    val flattendata3 = data3.withColumn("users", expr("explode(users)"))
                            .select(
                                col("page"),
                                col("per_page"),
                                col("total"),
                                col("total_pages"),
                                col("users.*")
                            )
    
    flattendata3.show()
    flattendata3.printSchema()
    
    val complexdata3 = flattendata3.groupBy("page", "per_page","total", "total_pages")
                                   .agg(collect_list(
                                   struct(
                                       
                                       col("emailAddress"),
                                       col("firstName"),
                                       col("lastName"),
                                       col("phoneNumber"),
                                       col("userId")
                                   )).as("users"))
                                   
    complexdata3.show()
    complexdata3.printSchema()
    
}
}