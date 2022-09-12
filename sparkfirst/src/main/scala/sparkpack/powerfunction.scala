package sparkpack

object powerfunction {
  
  def main(args:Array[String]):Unit={
    
    println(power(3,3)%5)
    
  }
  
  def power(A:Int, B:Int):Int={
    
    if(B==0){
      1
    }else{
    val halfpower = power(A, B/2)
    
    if(B%2==0){
      halfpower*halfpower
    }else
      halfpower*halfpower*A  
    }
  }
}