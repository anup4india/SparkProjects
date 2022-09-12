package zeyopack

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.functions._

object revision {

  case class schema(txnno: String, txndate: String, custno: String, amount: String, category: String, product: String, city: String, state: String, spendby: String)

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("revision").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    // Column List
    val collist = List("txnno", "txndate", "custno", "amount", "category", "product", "city", "state", "spendby")

    val lstint = List(1, 4, 6, 7)

    val newlst = lstint.map(x => x + 2)
    newlst.foreach(println)

    val lststr = List("zeyo", "analytics", "zeyobron", "bigdata")

    val newlststr = lststr.filter(x => x.contains("zeyo"))
    newlststr.foreach(println)

    // RDD from textfile
    val data = sc.textFile("file:///F:/Big Data/revdata/file1.txt")
    val filterrdd = data.filter(x => x.contains("Gymnastics"))

    filterrdd.take(10).foreach(println)

    // Schema RDD

    val schemardd = data.map(x => x.split(",")).map(x => schema(x(0), x(1), x(2), x(3), x(4), x(5), x(6), x(7), x(8)))

    val filterschemardd = schemardd.filter(x => x.category.contains("Gymnastics"))
    filterschemardd.foreach(println)

    // Row RDD

    val data1 = sc.textFile("file:///F:/Big Data/revdata/file2.txt")
    val rowrdd = data1.map(x => x.split(",")).map(x => Row(x(0), x(1), x(2), x(3), x(4), x(5), x(6), x(7), x(8)))
    rowrdd.foreach(println)

    // Dataframe using Schema RDD and Row RDD

    schemardd.toDF().show()

    val structschema = new StructType()
      .add("txnno", StringType)
      .add("txndate", StringType)
      .add("custno", StringType)
      .add("amount", StringType)
      .add("category", StringType)
      .add("product", StringType)
      .add("city", StringType)
      .add("state", StringType)
      .add("spendby", StringType)

    val schemaDF = spark.createDataFrame(rowrdd, structschema)
    schemaDF.show()

    // Dataframe using CSV file

    val csvdf = spark.read.format("csv").option("header", "true").load("file:///F:/Big Data/revdata/file3.txt").select(collist.map(col): _*)
    println("CSV file Dataframe")
    csvdf.show()

    // Dataframe using JSON file

    val jsondf = spark.read.format("json").load("file:///F:/Big Data/revdata/file4.json").select(collist.map(col): _*)

    println("JSON file Dataframe")
    jsondf.show()

    // Dataframe using parquet file

    val pardf = spark.read.load("file:///F:/Big Data/revdata/file5.parquet").select(collist.map(col): _*)

    println("Parquet file Dataframe")
    pardf.show()

    // Dataframe using xml file

    val xmldf = spark.read.format("com.databricks.spark.xml").option("rowTag", "txndata").load("file:///F:/Big Data/revdata/file6").select(collist.map(col): _*)

    println("XML file Dataframe")
    xmldf.show()

    val uniondf = csvdf.union(jsondf).union(pardf).union(xmldf)

    println("Union Dataframe")
    uniondf.show()

    val procdf = uniondf.withColumn("txndate", expr("split(txndate,'-')[2]"))
                        .withColumnRenamed("txndate", "year")
                        .withColumn("status",expr("case when spendby = 'cash' then 1 else 0 end"))
                        .filter(col("txnno") > 50000)

    println("Proccessed DF")
    procdf.show()
    
    // Aggregation sum of amount groupby category
    println("Groupby and aggregation")
    val sumdf = procdf.groupBy("category").agg(sum("amount").as("Total"))
    sumdf.show()
    //uniondf.printSchema()
    // Dataframe write as avro with partition
    sumdf.write.format("avro").partitionBy("category").mode("overwrite").save("file:///F:/Big Data/revdata/categorypartition1")
    println("Write completed")
    
    val jdbcdf = spark.read.format("jdbc")
      .option("url", "jdbc:mysql://zeyodb.chwk6wgpdls3.ap-south-1.rds.amazonaws.com:3306/zeyodata")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("user", "root")
      .option("password", "Aditya908")
      .option("dbtable", "rdstable")
      .load()
      .select(collist.map(col):_*)
      
      println("JDBC Dataframe")
      jdbcdf.show()
      

  }
}