package Job2

import HelperUtils.CreateLogger
import Job3.JobReducer
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.Reducer

import scala.collection.JavaConverters.*
import java.lang

class ReducerJob2_2 extends Reducer[IntWritable, Text, Text, IntWritable] {
  val logger = CreateLogger(classOf[ReducerJob2_2])
  override def reduce(key: IntWritable, values: lang.Iterable[Text], context: Reducer[IntWritable, Text, Text, IntWritable]#Context): Unit = {
    context.write(values.iterator().next(), key)
  }
}
