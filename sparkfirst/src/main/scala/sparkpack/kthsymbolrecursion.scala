package sparkpack

object kthsymbolrecursion {
  
  def main(args:Array[String]):Unit={

    println(solve(2,2))
    
  }
  
  def solve(n:Int,k:Int):Int={
    if(n == 1 && k==1) {
            0;
        }else{
        val mid = Math.pow(2,n-1)/2;
        val mid1 = mid.toInt
        if(k<=mid){
            solve(n-1,k);
        }else{
            1-solve(n-1,k-mid1);
        }
        }
  }
}
