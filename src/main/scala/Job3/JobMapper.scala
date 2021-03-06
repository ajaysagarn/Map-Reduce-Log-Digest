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
  val one = new IntWritable(1)

  override def map(key: Object, value: Text, context: Mapper[Object, Text, Text, IntWritable]#Context): Unit = {
    //split the log message into respective tokens based on the delimiter
    val tokens = new StringTokenizer(value.toString,"[] ")

    contextWriteRecursive(tokens, context)
  }

  /**
   * Recursively check all the tokens and write to context if there is a valid log type.
   * @param tokens
   * @param context
   */
  def contextWriteRecursive(tokens: StringTokenizer, context: Mapper[Object, Text, Text, IntWritable]#Context): Unit = {
    if(!tokens.hasMoreTokens()){
      return
    }
    val token: String = tokens.nextToken()
    if(MapReduceUtils.isValidLogLevel(token)){
      logType.set(token)
      logger.info("Add a mapped key {} with value {}",token, one.get())
      context.write(logType, one)
    }
    contextWriteRecursive(tokens, context)
  }
}  




