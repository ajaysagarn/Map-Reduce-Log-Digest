package Job1

import HelperUtils.{CreateLogger, MapReduceUtils, Parameters}
import Job2.MapperJob2
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.Mapper

import java.util.StringTokenizer

class MapperJob1 extends Mapper[Object, Text, Text, Text] {
  val logger = CreateLogger(classOf[MapperJob2])
  val logType = new Text()
  val logCount = new IntWritable()

  val interval = new Text()
  val map_out = new Text()

  override def map(key: Object, value: Text, context: Mapper[Object, Text, Text, Text]#Context): Unit = {
    //Get the timestamp for the log message
    val timestamp: Option[String] = MapReduceUtils.getLogTimeStamp(value.toString)
    //Get the log level of the log message
    val LogLevel = MapReduceUtils.getLogLevel(value.toString)
    //Get the actual message value for the log
    val message = MapReduceUtils.getLogMessage(value.toString)

    if(timestamp.nonEmpty){
      //Generate the interval string using the log's timestamp
      val intervalString = MapReduceUtils.getIntervalString(timestamp.get)
      if(intervalString.nonEmpty && !LogLevel.isEmpty){
          //set the timestamp as the key
          interval.set(intervalString.get)
          //combine the log level and the message as the mapper out val with " - " as a seperator
          map_out.set(LogLevel+" - "+message)
          logger.info("Map reduce job 2 setting key = {} and value = {}",interval,map_out)
          context.write(interval, map_out)
      }
    }
  }
}

