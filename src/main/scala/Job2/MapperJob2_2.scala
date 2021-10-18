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
    // retreives the key and the values pairs created by the previous map reduce job
    val tokens = new StringTokenizer(value.toString,"\t")
    // get the time interval value which is the first value in the output of the first map reduce job
    val interval = tokens.nextToken()
    // get the count fot the interval
    val count = Integer.parseInt(tokens.nextToken())
    intr.set(interval)

    logger.info("Addind new kv {} {} in second mapper",count, interval)
    // here we reverse the key value pairs from the previous map-reduce jobs. that way we can apply a sorting
    // comparator to perform the required sorting operations
    context.write(new IntWritable(count),intr)
  }
}
