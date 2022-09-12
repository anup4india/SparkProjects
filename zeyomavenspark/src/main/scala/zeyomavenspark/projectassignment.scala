package zeyomavenspark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.hive.conf.HiveConf

object projectassignment {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("project").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().getOrCreate()
    val folder = "file:///F:/Big Data/testfiles/"

    import spark.implicits._

    //val hdfsfile = spark.read.format("csv").option("header","true").load("hdfs://localhost:9000/hdfs/zeyodata1.txt")
    //hdfsfile.show()

    val rawdata = spark.read.format("csv").option("header", "true").load("file:///F:/Big Data/testfiles/allcountry.csv")

    //rawdata.show()

    import org.apache.hadoop.fs.{ FileSystem, Path }
    val fs = FileSystem.get(new java.net.URI("hdfs://localhost:9000"), spark.sparkContext.hadoopConfiguration)

    val gdata = rawdata.select("Country").distinct().collect().map(x =>

      {
        val y = x.getString(0)
        val filepath = new Path(s"/hdfs/$y/*")

        //if(fs.exists(filepath) && fs.isFile(filepath) )
        //fs.delete(filepath,true)

        //if(fs.exists(filepath) && fs.isFile(filepath) )
        fs.globStatus(filepath).map(_.getPath).foreach(println)
        //println(fs.globStatus(filepath).map(_.getPath).isEmpty) //map(x => fs.delete(x, true))

        println("Data deleted from " + y)

        //spark.read.format("csv").option("header","true").load(folder+y+"/*").show()
      })

    //gdata.foreach(println)

    // rawdata.show()

    //rawdata.write.format("csv").partitionBy("state","age").mode("overwrite").save("file:///F:/Big Data/testfiles/outputdata")
    //println("write completed")

    //val procdata = spark.read.format("csv").option("header", "true").load("file:///F:/Big Data/testfiles/usdata.csv")
    //println("proceessed data")
    //procdata.show()

  }
}