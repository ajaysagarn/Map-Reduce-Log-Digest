package Job4

import HelperUtils.{CreateLogger, MapReduceUtils}
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.Reducer

import java.lang
import scala.collection.JavaConverters.*

class LargestMsgReducer extends Reducer[Text, Text, Text, IntWritable] {
  val logger = CreateLogger(classOf[LargestMsgReducer])
  override def reduce(key: Text, values: lang.Iterable[Text], context: Reducer[Text, Text, Text, IntWritable]#Context): Unit = {
    val vals = values.iterator().asScala
    // Get the length of the largest string from the list of all messages for the log type.
    val maxCharacters: Int = MapReduceUtils.getMaxMessageCharacters(vals,0)
    logger.info("Maximum matched message for log level {} is {}",key.toString,maxCharacters)
    context.write(key, new IntWritable(maxCharacters))
  }


}
