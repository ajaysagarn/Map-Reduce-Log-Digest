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
    val LogLevel: String = MapReduceUtils.getLogLevel(value.toString)
    val logMessage: String = MapReduceUtils.getLogMessage(value.toString)

    logType.set(LogLevel)
    logmessage.set(logMessage)

    logger.info("Setting map key {} and value {}",logType.toString,logmessage.toString)

    if(!LogLevel.isEmpty && MapReduceUtils.doesContainPattern(logMessage,Parameters.generatingPattern.r))
      context.write(logType,logmessage)
  }
}
