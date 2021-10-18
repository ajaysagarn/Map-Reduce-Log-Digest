package Job2

import HelperUtils.{CreateLogger, MapReduceUtils, Parameters}
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.Mapper

import java.util.StringTokenizer

class MapperJob2 extends Mapper[Object, Text, Text, IntWritable] {
  val logger = CreateLogger(classOf[MapperJob2])
  val logType = new Text()
  val logCount = new IntWritable()

  val one = new IntWritable(1)
  val interval = new Text()

  override def map(key: Object, value: Text, context: Mapper[Object, Text, Text, IntWritable]#Context): Unit = {
    val tokens = new StringTokenizer(value.toString,"[] ")

    val timestamp: Option[String] = MapReduceUtils.getLogTimeStamp(value.toString)

    if(timestamp.nonEmpty){
      val intervalString = MapReduceUtils.getIntervalString(timestamp.get)
      if(intervalString.nonEmpty){
        val logLevel = MapReduceUtils.getLogLevel(value.toString)
       // check of the lof type is of the tpye "ERROR" and also check of the log message matches with the regex pattern specified
        if(logLevel == "ERROR" && MapReduceUtils.doesContainPattern(value.toString,Parameters.generatingPattern.r)){
          interval.set(intervalString.get)
          //Set the interval string as the key and 1 as the output value of the mapper
          logger.info("Map reduce job2-1 writing key = {} value = {}", interval, one)
          context.write(interval,one)
        }
      }
    }

  }
}
