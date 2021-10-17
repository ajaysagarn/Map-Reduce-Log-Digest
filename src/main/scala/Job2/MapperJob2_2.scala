package Job2

import HelperUtils.{CreateLogger, MapReduceUtils, Parameters}
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.Mapper

import java.util.StringTokenizer

class MapperJob2_2 extends Mapper[Object, Text, IntWritable, Text] {
  val logger = CreateLogger(classOf[MapperJob2_2])
  val logType = new Text()
  val logCount = new IntWritable()

  val one = new IntWritable(1)
  val intr = new Text()

  override def map(key: Object, value: Text, context: Mapper[Object, Text, IntWritable, Text]#Context): Unit = {

    val tokens = new StringTokenizer(value.toString,"\t")
    val timestamp: Option[String] = MapReduceUtils.getLogTimeStamp(value.toString)
    val interval = tokens.nextToken()
    val count = Integer.parseInt(tokens.nextToken())
    intr.set(interval)

    logger.info("Addind new kv {} {} in second mapper",count, interval)
    context.write(new IntWritable(count),intr)
  }
}
