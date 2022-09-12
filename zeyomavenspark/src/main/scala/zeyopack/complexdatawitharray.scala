package zeyopack

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object complexdatawitharray {

	def main(args:Array[String]):Unit={

			val conf = new SparkConf().setAppName("complexdata").setMaster("local[*]")
					val sc = new SparkContext(conf)
					sc.setLogLevel("ERROR")

					val spark = SparkSession.builder().getOrCreate()

					val data = spark.read.format("json").option("multiline", "true").load("file:///F:/Big Data/Complex Data/zeyoc.json")

					data.show()
					data.printSchema()

					val flattendata = data.select(

							explode(col("Students")).as("Students"),
							col("address.*"),
							col("orgname"),
							col("trainer")
							)

					flattendata.show()
					flattendata.printSchema()
					
					val data1 = spark.read.format("json").option("multiline", "true").load("file:///F:/Big Data/Complex Data/zeyoc.json")
					
					
					val flattendata1 = data1.withColumn("Students", expr("explode(Students)"))
					                        .withColumn("permanentAddress", expr("address.permanentAddress"))
					                        .withColumn("temporaryAddress",expr("address.temporaryAddress"))
					                        .drop("address")
					                        
					
					flattendata1.show()
					flattendata1.printSchema()

	}
}