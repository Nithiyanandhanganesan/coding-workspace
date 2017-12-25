

object Test1 {
  
   def main(args: Array[String]) {
      var a = Array(1,2,3,4,5,6);
      var b = Array(5,6,7,8,9,7);
      var i,j=0;
      for (i <- 0 until a.length)
      {
         print("i=",a(i),"j=",b(j));
         j=j+1;
      }
    }
}