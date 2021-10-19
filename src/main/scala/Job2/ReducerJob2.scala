package Job2

import HelperUtils.CreateLogger
import Job3.JobReducer
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.Reducer

import java.lang
import scala.collection.JavaConverters.*

class ReducerJob2 extends Reducer[Text, IntWritable, Text, IntWritable] {
  val logger = CreateLogger(classOf[JobReducer])
  override def reduce(key: Text, values: lang.Iterable[IntWritable], context: Reducer[Text, IntWritable, Text, IntWritable]#Context): Unit = {
    val sum = values.asScala.foldLeft(0)(_ + _.get)
    logger.info("Producing reducer 1 output key = {} valule = {}", key, sum)
    context.write(key, new IntWritable(sum))
  }
}

