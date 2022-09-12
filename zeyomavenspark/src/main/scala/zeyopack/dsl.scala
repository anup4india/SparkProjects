package zeyopack

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.derby.impl.sql.compile.OrderByColumn
import org.apache.spark.sql.expressions.Window

object dsl {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("DSL").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().getOrCreate()

    import spark.implicits._

    val df = spark.read.format("csv").option("header", "true").option("delimiter", "~").load("file:///F:/Big Data/testfiles/txns_head")
    df.show()
    
    val window = Window.partitionBy("category").orderBy(col("amount").desc)
    
    df.withColumn("Rank", dense_rank().over(window)).show() //.filter("Rank=2").groupBy("category").agg(avg("amount")).show()
    //println(df.groupBy("category").agg(sum("amount")).first())
    df.groupBy("category").agg(sum("amount")).show()

    /* // Multicolumn Filter
    df.filter(

        col("category") === "Gymnastics"
        &&
        col("spendby") === "cash"

              ).show()

    // Multivalue Filter
    df.filter(

              col("category").isin("Gymnastics", "Team Sports")
              &&
              col("product") like ("%Gymnastics%")

              ).show()*/

    val df1 = spark.read.format("json").load("file:///F:/Big Data/testfiles/devices.json")
    //df1.show()

    //df1.filter(col("lat") > 70).write.format("parquet").save("file:///F:/Big Data/testfiles/filterdatapar")

    val procdata = df.selectExpr(
      "txnno",
      "split(txndate,'-')[2] as year",
      //"split(txndate,'-')[1] as month",
      //"split(txndate,'-')[0] as day",
      "custno",
      "amount",
      "category",
      "product",
      "city",
      "state",
      "spendby",
      "case when spendby = 'cash' then 0 else 1 end AS check")
    //procdata.show()

    val procdata1 = df
      .withColumn("txndate", expr("split(txndate,'-')[2]"))
      .withColumnRenamed("txndate", "year")
      .withColumn("check", expr("case when spendby = 'cash' then 0 else 1 end"))

    //procdata1.show()

    val procdata2 = df
      .withColumn("city", expr("concat(city,'-',state)"))
      .withColumnRenamed("city", "location")
      .drop("state")

    //procdata2.show()

    val df2 = spark.read.format("csv").option("header", "true").load("file:///F:/Big Data/testfiles/zeyodata1.txt")
    df2.show()
    //val groupdata = df2.groupBy("name").agg(sum("amount"))
    val groupdata = df2.groupBy("name").agg(sum("amount").alias("total"), count("product").alias("product_count"))
    groupdata.show()
    
    val groupdata1 = spark.read.format("csv").option("header", "true").load("file:///F:/Big Data/testfiles/txns_head")
    
    
    

  }
}