package Job3

import HelperUtils.CreateLogger
import org.apache.hadoop.io.{IntWritable, LongWritable, Text}
import org.apache.hadoop.mapreduce.Reducer

import java.{lang, util}
import scala.collection.JavaConverters.*


class JobReducer extends Reducer[Text, IntWritable, Text, IntWritable] {
  val logger = CreateLogger(classOf[JobReducer])
  override def reduce(key: Text, values: lang.Iterable[IntWritable], context: Reducer[Text, IntWritable, Text, IntWritable]#Context): Unit = {
    var sum = values.asScala.foldLeft(0)(_ + _.get)
    logger.info("Producing reducer output key={} value = {}",key, sum)
    context.write(key, new IntWritable(sum))
  }
}




