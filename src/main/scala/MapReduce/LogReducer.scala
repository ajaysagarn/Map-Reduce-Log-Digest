package MapReduce

import HelperUtils.CreateLogger
import org.apache.hadoop.io.{IntWritable, LongWritable}
import org.apache.hadoop.mapreduce.Reducer
import org.w3c.dom.Text

import java.{lang, util}

class LogReducer extends Reducer[Text, IntWritable, Text, IntWritable] {
  val logger = CreateLogger(classOf[LogReducer])
  
  override def reduce(key: Text, values: lang.Iterable[IntWritable], context: Reducer[Text, IntWritable, Text, IntWritable]#Context): Unit = {
    super.reduce(key, values, context)
  }
}
