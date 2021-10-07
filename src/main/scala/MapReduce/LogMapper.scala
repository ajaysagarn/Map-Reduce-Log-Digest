package MapReduce

import Generation.LogMsgSimulator
import HelperUtils.CreateLogger
import org.apache.hadoop.io.{IntWritable, LongWritable, Text}
import org.apache.hadoop.mapreduce.Mapper

class LogMapper extends Mapper[Object, Text, Text, IntWritable] {

  val logger = CreateLogger(classOf[LogMsgSimulator])
  val logType = new Text()
  val logCount = new IntWritable()


  override def map(key: Object, value: Text, context: Mapper[Object, Text, Text, IntWritable]#Context): Unit = {
    super.map(key, value, context)
    logger.info(value.toString)
  }

/*  override def map(key: LongWritable, value: Text, context: Mapper[LongWritable, Text, Text, IntWritable]#Context): Unit = {
    logger.info(value.toString())

    logType.set(value.toString)
    logCount.set(1)


  }*/
}
