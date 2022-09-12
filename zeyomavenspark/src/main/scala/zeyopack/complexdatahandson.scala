package zeyopack

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object complexdatahandson {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("complexdata").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    val data = spark.read.format("json").option("multiline", "true").load("file:///F:/Big Data/Complex Data/zeyoc3.json")

    data.show()
    data.printSchema()

    val flattendata = data.withColumn("Students", expr("explode(Students)"))
      .select(

        col("orgname"),
        col("trainer"),
        col("Students.user.*"),
        col("address.*"))

    flattendata.show()
    flattendata.printSchema()

  }

}