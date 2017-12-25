
import org.apache.spark.sql.functions._
import java.util.UUID

import java.text.SimpleDateFormat
import java.util.Date
import java.util.Calendar
import java.time.LocalDate
import scala.collection.mutable.MutableList

object Test {
  def main(args: Array[String]) {
    //var s="bilda.donado@yahoo.com"
    //val test=UUID.nameUUIDFromBytes(s.getBytes).toString;
    //print(test);
    /*var startDate = "2016-01-01";  // Start date
    val dateFormat= new SimpleDateFormat("yyyy-MM-dd");
    val currentDate = dateFormat.format(new Date())
    val cal = Calendar.getInstance();
    cal.setTime(dateFormat.parse(startDate));
    cal.add(Calendar.DATE, 1);  // number of days to add
    startDate = dateFormat.format(cal.getTime());  // dt is now the new date
    //val currentDate1 = dateFormat.parse(currentDate);
    
    val test= startDate - currentDate
    print(startDate,currentDate.toString())*/
    
  def genDateRange (startDate: Date, endDate: Date)= {
    var  simpleDateFormat:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
    var dt = startDate
 //   val res: MutableList[Date] = new MutableList[Date]()
    val c = Calendar.getInstance()
    while ( dt.before(endDate) || dt.equals(endDate)) {
  //    res += dt
      c.setTime(dt)
      c.add(Calendar.DATE, 1)
      dt = c.getTime
      dt.formatted("yyyy-MM-dd")
      val loadYear = new SimpleDateFormat("yyyy").format(dt) 
      val loadMonth = new SimpleDateFormat("MM").format(dt) 
      val loadDate = new SimpleDateFormat("dd").format(dt) 
      println(loadYear+"/"+loadMonth+"/"+loadDate)

    }
//    res.toList
  }
   
  
  val start = "2016-01-01";
  val dateFormat = new SimpleDateFormat("yyyy-MM-dd");
  val startDate = dateFormat.parse(start);
  val current = new SimpleDateFormat("yyyy-MM-dd").format(new Date());
  val endDate = dateFormat.parse(current)
  
  
  

    genDateRange(startDate, endDate)
  
  }
}