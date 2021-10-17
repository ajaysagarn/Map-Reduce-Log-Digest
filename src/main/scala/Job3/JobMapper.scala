package Job3

import Generation.LogMsgSimulator
import HelperUtils.{CreateLogger, MapReduceUtils}
import com.sun.tools.javac.parser.Tokens
import org.apache.hadoop.io.{IntWritable, LongWritable, Text}
import org.apache.hadoop.mapreduce.Mapper

import java.util.StringTokenizer

class JobMapper extends Mapper[Object, Text, Text, IntWritable] {

  val logger = CreateLogger(classOf[LogMsgSimulator])
  val logType = new Text()
  val logCount = new IntWritable()

  val one = new IntWritable(1)
  val word = new Text()

  override def map(key: Object, value: Text, context: Mapper[Object, Text, Text, IntWritable]#Context): Unit = {
    val tokens = new StringTokenizer(value.toString,"[] ")
    contextWriteRecursive(tokens, context)
  }

  def contextWriteRecursive(tokens: StringTokenizer, context: Mapper[Object, Text, Text, IntWritable]#Context): Unit = {
    if(!tokens.hasMoreTokens()){
      return
    }
    val token: String = tokens.nextToken()
    if(MapReduceUtils.isValidLogLevel(token)){
      word.set(token)
      logger.info("Add a mapped key {} with value {}",token, one.get())
      context.write(word, one)
    }
    contextWriteRecursive(tokens, context)
  }
}  




