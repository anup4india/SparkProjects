package zeyomavenspark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.{StringType,LongType}

object sparkperformancetuning {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("spark_performance").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().appName("spark").master("local[*]").getOrCreate()

    import spark.implicits._

    // Left Anti Join
    

    val df1 = spark.read.format("csv").option("header", "true").load("file:///F:/Big Data/testfiles/data1.txt")
    df1.show()

    val df2 = spark.read.format("csv").option("header", "true").load("file:///F:/Big Data/testfiles/data2.txt")
    df2.show()

    val list = df2.select("id").map(x => x.mkString("")).collect().toList
    list.foreach(println)

    val df3 = df1.filter(!(col("id").isin(list: _*)))
    df3.show()

    //Left Anti Join
    val df4 = df1.join(df2, df1.col("id")===df2.col("id"), "left_anti")
    val df4a = df1.except(df2)
    println("except")
    df4a.show()
    //val df4 = df1.join(df2, Seq("id"), "left_anti")
    
    
    println("left_anti join")
    df4.show()
    
    //Broadcast Join
    val df41 = df1.join(broadcast(df2),Seq("id"))
    println("Broadcast Join")
    df41.drop(df2.col("name")).show()

    // Zip with Index

    val df5 = df1.withColumn("index", monotonically_increasing_id() + 1)
    df5.show()

    val df6 = spark.sqlContext.createDataFrame(

      df1.rdd.zipWithIndex.map {

        case (row, index) => Row.fromSeq(row.toSeq :+ index)
      }, StructType(df1.schema.fields :+ StructField("index", LongType, false)))

    df6.show()
    val struct1 = new StructType().add("id",StringType)
    val columns = Seq("id")
    val data = Seq(Row("1"),Row("2"),Row("3"))
    val tst = spark.createDataFrame(spark.sparkContext.parallelize(data),struct1)
    tst.show()
    println(spark, spark.version)
    
    val a = spark.sparkContext.accumulator(0,"a")

  }
}