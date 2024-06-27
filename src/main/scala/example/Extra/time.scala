package example.Extra

import java.time.LocalDate
import java.time.format.DateTimeFormatter

object time extends App{
  def getTime():String={
    val currentDate = LocalDate.now()
    // Format the date to yy-mm-dd format
    val formattedDate = currentDate.format(DateTimeFormatter.ofPattern("yy-MM-dd"))
    // Print the formatted date
    formattedDate
  }
  println(getTime())
}
