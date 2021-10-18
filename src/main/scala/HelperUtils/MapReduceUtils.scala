package HelperUtils

import Job1.LogTypes
import org.apache.hadoop.io.Text

import java.util.StringTokenizer
import java.util.regex.Pattern
import scala.util.matching.Regex

class MapReduceUtils
object MapReduceUtils {

  /**
   *Checks if the input string is a valid Log type or not
   * @param token
   * @return
   */
  def isValidLogLevel (token:String): Boolean= token match {
    case "DEBUG" => true
    case "INFO" => true
    case "WARN" => true
    case "ERROR" => true
    case _ => false
  }

  /**
   * Get the log level if it exists in the string passed
   * @param token
   * @return String
   */
  def getLogLevel (token:String): String ={
    val tokens = new StringTokenizer(token," ")
    getIfLogLevel(tokens)
  }

  /**
   * Recursively loop through a list of string tokens and determine if any of them are valid log types
   * returns a valid log type if it exists in the list of tokens
   * @param tokens
   * @return
   */
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

  /**
   * Get the message part of the log string
   * @param log
   * @return
   */
  def getLogMessage(log: String): String = {
    val parts: Array[String] = log.split(" - ").map(str => str.trim())
    val message = parts(parts.length -1)
    message
  }

  /**
   * Check if the given string consists the given regex pattern in it
   * @param message
   * @param pattern
   * @return - Boolean
   */
  def doesContainPattern(message:String, pattern:Regex): Boolean = {
    !pattern.findFirstIn(message).isEmpty
  }

  /**
   * Retreive the timestamp string from the Log String
   * Uses a regex pattern to get the required timestamp
   * @param message
   * @return
   */
  def getLogTimeStamp(message:String): Option[String] = {
    // Regex pattern to match the time stamp in a log string
    val pattern = new Regex("[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{3}")
    pattern.findFirstIn(message)
  }


  /**
   * Function takes in a valid timestamp string and produces a string that depicts the time interval to which
   * the perticular timestamp belongs to based on the global time interval config value set
   * @param timestamp
   * @return
   */
  def getIntervalString(timestamp: String): Option[String] = {
    //Split the timestamp into respective parts
    val parts: Array[String] = timestamp.split(':')
    //get the hours value
    val hours: Int = Integer.parseInt(parts(0))
    //get the minutes value
    val minutes: Int = Integer.parseInt(parts(1))
    // Get the global time interval value set in the config file
    val interval = Parameters.timeInterval
    
    //Get the end time of the time interval that the current timestamp belongs to
    //Here we multiply (hours*60)+minutes to get the number of minutes passed since 00:00.
    val intervalEnd = getTimeIntervalEndingRecursive((hours * 60) + minutes , interval)
    
    //Once we have the end time as minutes from 00:00 we get the start time by substracting the endtime by the 
    //time interval set in the config file
    val intervalStart = convertMinutesToTimeString(intervalEnd - interval, interval)

    //Finally construct the time interval string
    val intervalString: Option[String] = Option.apply(intervalStart+" - "+ convertMinutesToTimeString(intervalEnd,interval))
    intervalString
  }

  /**
   * Retreives the end time in number of minutes from 00:00, using the time interval value 
   * to determine the correct time interval
   * @param minutes
   * @param interval
   * @return
   */
  def getTimeIntervalEndingRecursive(minutes:Int, interval: Int): Int ={
    if( (minutes >= interval) && (minutes % interval) == 0){
      return minutes
    }
    //increment the munites until you find one that is completely divisible by the time interval value
    getTimeIntervalEndingRecursive(minutes + 1 , interval)
  }

  /**
   * Convert minutes to the correct time format.
   * @param minutes
   * @param interval
   * @return
   */
  def convertMinutesToTimeString(minutes: Int, interval: Int): String ={
    if(minutes == 0) {
      "00:00"
    } else {
      val hour: Int = minutes / 60
      val mins: Int = minutes - (hour * 60)
      hour+":"+mins
    }
  }

  /**
   * Get the length of the maximum string from a list of Strings
   * @param iterator
   * @param max
   * @return
   */
  def getMaxMessageCharacters(iterator:Iterator[Text], max: Int): Int = {
    if(!iterator.hasNext)
      return max
    
    // get the next string
    val message = Option.apply(iterator.next())
    //check if the new string's length is greater than the current max value
    if(message.nonEmpty && message.get.toString.length > max)
      getMaxMessageCharacters(iterator, message.toString.length) // replace the max value for the next iteration
    else
      getMaxMessageCharacters(iterator, max) //continue with the same max value
  }

  /**
   * Aggregates a list of intermediate values to find out the number of messages that belong to
   * each Log Type, and also finc out the number of messages of each Log Type that match the specified
   * Regex pattern
   * @param iterator
   * @param logTypes
   * @return
   */
  def getLogTypesRecursive(iterator: Iterator[Text], logTypes: LogTypes): LogTypes ={
    if(!iterator.hasNext){
      return logTypes // return the aggregated values.
    }
    
    //retreive the corresponding values from the String - here the format received from the mapper is <Logtype> - <The message value>
    val split: Array[String] = iterator.next().toString.split(" - ")

    // Aggregate the values by matching the Log type and checking if the regex pattern matches with the message
    //for each of the messages.
    split(0) match {
      case "ERROR" => {
        if(MapReduceUtils.doesContainPattern(split(1), Parameters.generatingPattern.r))
          getLogTypesRecursive(iterator: Iterator[Text], new LogTypes(logTypes.warn,logTypes.info,logTypes.debug,logTypes.error + 1,logTypes.warnMatch,logTypes.infoMatch,logTypes.debugMatch,logTypes.errorMatch +1))
        else
          getLogTypesRecursive(iterator: Iterator[Text], new LogTypes(logTypes.warn,logTypes.info,logTypes.debug,logTypes.error + 1,logTypes.warnMatch,logTypes.infoMatch,logTypes.debugMatch,logTypes.errorMatch))
      }
      case "DEBUG" => {
        if(MapReduceUtils.doesContainPattern(split(1),Parameters.generatingPattern.r))
          getLogTypesRecursive(iterator: Iterator[Text], new LogTypes(logTypes.warn,logTypes.info,logTypes.debug+1,logTypes.error,logTypes.warnMatch,logTypes.infoMatch,logTypes.debugMatch+1,logTypes.errorMatch))
        else
          getLogTypesRecursive(iterator: Iterator[Text], new LogTypes(logTypes.warn,logTypes.info,logTypes.debug+1,logTypes.error,logTypes.warnMatch,logTypes.infoMatch,logTypes.debugMatch,logTypes.errorMatch))
      }
      case "INFO" => {
        if(MapReduceUtils.doesContainPattern(split(1),Parameters.generatingPattern.r))
          getLogTypesRecursive(iterator: Iterator[Text], new LogTypes(logTypes.warn,logTypes.info+1,logTypes.debug,logTypes.error,logTypes.warnMatch,logTypes.infoMatch+1,logTypes.debugMatch,logTypes.errorMatch))
        else
          getLogTypesRecursive(iterator: Iterator[Text], new LogTypes(logTypes.warn,logTypes.info+1,logTypes.debug,logTypes.error,logTypes.warnMatch,logTypes.infoMatch,logTypes.debugMatch,logTypes.errorMatch))
      }
      case "WARN" => {
        if(MapReduceUtils.doesContainPattern(split(1),Parameters.generatingPattern.r))
          getLogTypesRecursive(iterator: Iterator[Text], new LogTypes(logTypes.warn+1,logTypes.info,logTypes.debug,logTypes.error,logTypes.warnMatch+1,logTypes.infoMatch,logTypes.debugMatch,logTypes.errorMatch))
        else
          getLogTypesRecursive(iterator: Iterator[Text], new LogTypes(logTypes.warn+1,logTypes.info,logTypes.debug,logTypes.error,logTypes.warnMatch,logTypes.infoMatch,logTypes.debugMatch,logTypes.errorMatch))
      }
    }
  }


}
