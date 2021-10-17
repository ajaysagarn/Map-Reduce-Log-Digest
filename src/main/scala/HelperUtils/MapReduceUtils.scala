package HelperUtils

import org.apache.hadoop.io.Text

import java.util.StringTokenizer
import java.util.regex.Pattern
import scala.util.matching.Regex

class MapReduceUtils
object MapReduceUtils {

  def isValidLogLevel (token:String): Boolean= token match {
    case "DEBUG" => true
    case "INFO" => true
    case "WARN" => true
    case "ERROR" => true
    case _ => false
  }

  def getLogLevel (token:String): String ={
    val tokens = new StringTokenizer(token," ")
    getIfLogLevel(tokens)
  }


  def getIfLogLevel(tokens: StringTokenizer): String = {
    if(!tokens.hasMoreTokens()){
      return ""
    }
    val token: String = tokens.nextToken()
    if(isValidLogLevel(token)){
      return token
    }
    getIfLogLevel(tokens)
  }

  def getLogMessage(log: String): String = {
    val parts: Array[String] = log.split(" - ").map(str => str.trim())
    val message = parts(parts.length -1)
    message
  }

  def doesContainPattern(message:String, pattern:Regex): Boolean = {
    !pattern.findFirstIn(message).isEmpty
  }


  def getLogTimeStamp(message:String): Option[String] = {
    // Regex pattern to match the time stamp in a log string
    val pattern = new Regex("[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{3}")
    pattern.findFirstIn(message)
  }


  def getIntervalString(timestamp: String): Option[String] = {
    // get split the interval string and get the hours and minutes.
    // using the minutes value and the interval set in the config, construct the time interval string

    val parts: Array[String] = timestamp.split(':')
    val hours: Int = Integer.parseInt(parts(0))
    val minutes: Int = Integer.parseInt(parts(1))
    val interval = Parameters.timeInterval
    val timestampMinutes = hours * minutes
    val intervalEnd = getTimeIntervalEndingRecursive((hours * 60) + minutes , interval)
    val intervalStart = convertMinutesToTimeString(intervalEnd - interval, interval)

    val intervalString: Option[String] = Option.apply(intervalStart+" - "+ convertMinutesToTimeString(intervalEnd,interval))

    intervalString
  }


  def getTimeIntervalEndingRecursive(minutes:Int, interval: Int): Int ={
    if( (minutes >= interval) && (minutes % interval) == 0){
      return minutes
    }
    getTimeIntervalEndingRecursive(minutes + 1 , interval)
  }

  def convertMinutesToTimeString(minutes: Int, interval: Int): String ={
    if(minutes == 0) {
      "00:00"
    } else {
      val hour: Int = minutes / 60
      val mins: Int = minutes - (hour * 60)
      hour+":"+mins
    }
  }

  def getMaxMessageCharacters(iterator:Iterator[Text], max: Int): Int = {
    if(!iterator.hasNext)
      return max

    val message = Option.apply(iterator.next())
    if(message.nonEmpty && message.get.toString.length > max)
      getMaxMessageCharacters(iterator, message.toString.length)
    else
      getMaxMessageCharacters(iterator, max)
  }


}
