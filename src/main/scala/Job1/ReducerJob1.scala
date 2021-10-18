package Job1

import HelperUtils.{CreateLogger, MapReduceUtils, Parameters}
import Job3.JobReducer
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.Reducer

import scala.collection.JavaConverters.*
import java.lang

class ReducerJob1 extends Reducer[Text, Text, Text, Text] {
  val logger = CreateLogger(classOf[JobReducer])

  override def reduce(key: Text, values: lang.Iterable[Text], context: Reducer[Text, Text, Text, Text]#Context): Unit = {

    logger.info("Performing the reducer operation from key = {}", key)
    val iterator = values.iterator().asScala

    //compute the aggregationg for all the log message within one time interval
    val logTypes: LogTypes = MapReduceUtils.getLogTypesRecursive(iterator, new LogTypes(0,0,0,0,0,0,0,0))
    
    // write the results seperately for each Log type for each time interval.
    context.write(key, new Text("WARN,"+logTypes.warn+","+logTypes.warnMatch) )
    context.write(key, new Text("DEBUG,"+logTypes.debug+","+logTypes.debugMatch))
    context.write(key, new Text("ERROR,"+logTypes.error+","+logTypes.errorMatch))
    context.write(key, new Text("INFO,"+logTypes.warn+","+logTypes.warnMatch))
  }


}


