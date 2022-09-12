package sparkpack

object fibonacci {
  
  def main(args:Array[String]):Unit={
    
    println(fab(20))
  }
  
  def fab(N:Int): Int={
    
    if(N<=0){
      0
    }else

    if(N<=2){
      1
    }else
      
    fab(N-2)+fab(N-1)
  }
  
}