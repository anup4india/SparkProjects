package sparkpack

object scalerprog {
  
  def main(args:Array[String]):Unit={
    
    val A = 12345672
    if(sum(A)==1){
      println(1)
    }else
      println(0)
    
             
  }
  
  def sum(A:Int):Int = {
    
    if(A==0){
      0
    }else
      if((sum(A/10)+A%10)/10 != 0){
        sum(sum(A/10)+A%10)
      }else
     sum(A/10)+A%10
    
    
  }
}

  /*  val A = -1
    val B = 1
    val C = 20
    if(A==0){
      0
    }else
    if(A<0){
      println(C - power(-A,B,C))
    } else
    println(power(A,B,C))
        
  }
  
  def power(A:Int,B:Int,C:Int): Int = {
    
    if(B==0){
      1
    } else
    
    power(A,B-1,C)%C*A%C
      
    
  }
  */
