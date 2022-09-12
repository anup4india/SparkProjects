package sparkpack

object reverseorder {
  
  def main(args:Array[String]):Unit={
    val A = "ANUP CHAUHAN"
    val B = A.toCharArray()
    val N = B.length-1
    reverse(B,N)
  }
  
  def reverse(A:Array[Char],N:Int):Unit={
    if(N>=0){
    println(A(N))
    reverse(A,N-1)
    }
  }
}