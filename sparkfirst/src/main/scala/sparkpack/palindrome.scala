package sparkpack

object palindrome {
  
  def main(args:Array[String]):Unit={
    
    val B = "MADAM"
    val A = B.toCharArray()
    val i = 0
    val j = A.length-1
    
    println(compare(A,i,j))
  }
  
  def compare(A:Array[Char],i:Int,j:Int):Int={
    
       
    if(i>=j){
      1
    }else
     if(A(i).equals(A(j))){    
       compare(A,i+1,j-1)
    }else
      0
      
  }
}