package zeyopack

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import scala.io.Source
import org.apache.spark.sql.functions._

object jsonreadfromurl {
  
    def main(agrs:Array[String]):Unit={
    
    val conf = new SparkConf().setAppName("complexdata").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    
    val spark = SparkSession.builder().getOrCreate()
    
    import spark.implicits._
    
    val urldata = Source.fromURL("https://randomuser.me/api/0.8/?results=10").mkString
    
    val datardd = sc.parallelize(List(urldata))
    
    val datadf = spark.read.json(datardd)
    
    datadf.printSchema()
    
    val flattendata = datadf.withColumn("results",expr("explode(results)"))
                            .select(
                            col("nationality"),    
                            col("results.user.cell"),
                            col("results.user.dob"),
                            col("results.user.email"),
                            col("results.user.gender"),
                            col("results.user.location.*"),
                            col("results.user.md5"),
                            col("results.user.name.*"),
                            col("results.user.password"),
                            col("results.user.phone"),
                            col("results.user.picture.*"),
                            col("results.user.registered"),
                            col("results.user.salt"),
                            col("results.user.sha1"),
                            col("results.user.sha256"),
                            col("results.user.username"),
                            col("seed"),
                            col("version")
                            )
                            
    flattendata.show()
    flattendata.printSchema()
    
  }
}