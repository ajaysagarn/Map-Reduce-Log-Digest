package Job4

import HelperUtils.CreateLogger
import Job2.{MapperJob2, MapperJob2_2, ReducerJob2, ReducerJob2_2}
import Job3.JobRunner.logger
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat

/**
 * Produce the number of characters in each log message for each log message type
 * that contain the highest number of characters in the detected instances of the designated regex pattern.
 */
class JobRunner
object JobRunner {
  val logger = CreateLogger(classOf[JobRunner])

  def main(args: Array[String]): Unit  = {
    logger.info("Initializing map reduce job 4")
    // Read the default configuration of the cluster from configuration xml files
    val configuration = new Configuration
    configuration.set("mapred.textoutputformat.separator", ",")
    // Initialize the job with default configuration of the cluster
    val job = Job.getInstance(configuration, "MAX_ERROR_STRING_LENGTH")

    // Assign the drive class to the job
    job.setJarByClass(this.getClass)

    // Assign user-defined Mapper and Reducer class
    job.setMapperClass(classOf[LargestMsgMapper])
    job.setReducerClass(classOf[LargestMsgReducer])

    // Set the Key and Value types of the output
    job.setMapOutputKeyClass(classOf[Text])
    job.setMapOutputValueClass(classOf[Text])
    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[IntWritable])

    // Add input and output path from the args
    FileInputFormat.addInputPath(job, new Path(args(0)))
    FileOutputFormat.setOutputPath(job, new Path(args(1)))
    logger.info("Started map reduce job 4")
    System.exit(if(job.waitForCompletion(true)) 0 else 1)

  }

}


