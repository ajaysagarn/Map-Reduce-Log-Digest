package Job4

import Generation.LogMsgSimulator
import HelperUtils.{CreateLogger, MapReduceUtils, Parameters}
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.Mapper

import scala.collection.JavaConverters.*

class LargestMsgMapper extends Mapper[Object, Text, Text, Text] {

  val logger = CreateLogger(classOf[LargestMsgMapper])
  val logType = new Text()
  val logmessage = new Text()

  override def map(key: Object, value: Text, context: Mapper[Object, Text, Text, Text]#Context): Unit = {
    //Get the log level from the log string
    val LogLevel: String = MapReduceUtils.getLogLevel(value.toString)
    //Get the message part the log string
    val logMessage: String = MapReduceUtils.getLogMessage(value.toString)

    logType.set(LogLevel)
    logmessage.set(logMessage)

    logger.info("Setting map key {} and value {}",logType.toString,logmessage.toString)

    //check of its a valid log type and check if the message has the regex pattern specified
    if(!LogLevel.isEmpty && MapReduceUtils.doesContainPattern(logMessage,Parameters.generatingPattern.r))
      context.write(logType,logmessage) // write the log type as key and the message as the value
  }
}
