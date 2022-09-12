package sparkpack
import org.apache.spark._

object sparkobj {
  
  def main(args:Array[String]):Unit={
    
    val conf = new SparkConf().setAppName("first").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    
    val liststr=List(
			    "State->Telangana~City->Hyderabad",
					"State->Telangana~City->Hyderabad", 				
					"State->TamilNadu~City->Chennai", 
					"State->Karnataka~City->Bangalore"
					)
		
		val allDetails = sc.parallelize(liststr)
		
		val statelist = allDetails.flatMap(x => x.split("~")).filter(y => y.contains("State")).map(z => z.replace("State->", "")).distinct()
		statelist.foreach(println)
    
		val citylist = allDetails.flatMap(x => x.split("~")).filter(y => y.contains("City")).map(z => z.replace("City->", "")).distinct()
		citylist.foreach(println)

  }
  
}