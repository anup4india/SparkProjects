package sparkpack

object factorial {
  
  def main(args:Array[String]): Unit={
    println(fact(1))
  }
  
  def fact(N:Int):Int={
    
    if(N>0){
    fact(N-1)*N
    }else
    1
  }
}