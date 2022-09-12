package zeyomavenspark
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object handson {

  def main(args:Array[String]):Unit={
    
    val conf = new SparkConf().setMaster("local[*]").setAppName("Handson")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    
    //val data = sc.textFile("file:///F:/Big Data/Handson Data/usdata.csv")
    val data = sc.textFile("file:///F:/Big Data/testfiles/txnsmall.txt")
    
    //data.filter(x => x.length() > 200).flatMap(x => x.split(",")).map(x => x.concat(",zeyo")).map(x => x.replace("-", "")).coalesce(1).saveAsTextFile("file:///F:/Big Data/Handson Data/outputdata1")
    val gymdata = data.filter(x => x.contains("Gymnastics"))
    println("===== Gymnastic Data ======")
    gymdata.foreach(println)
    val teamdata = data.filter(x => x.contains("Team Sports"))
    println("===== Team Sports Data ======")
    teamdata.foreach(println)
    println("======= RDD Union ========")
    val uniondata = gymdata.union(teamdata)
    uniondata.foreach(println)
    
 /* (Directed Acyclic Graph) DAG in Apache Spark is a set of Vertices and Edges, 
  * where vertices represent the RDDs and the edges represent the Operation to be applied on RDD. 
  * In Spark DAG, every edge directs from earlier to later in the sequence. On the calling of Action, 
  * the created DAG submits to DAG Scheduler which further splits the graph into the stages of the task. */       
    
  }
}