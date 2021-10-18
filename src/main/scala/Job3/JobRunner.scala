package Job3

import HelperUtils.CreateLogger
import Job2.JobRunner.logger
import Job3.JobReducer
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.{Job, Mapper, Reducer}

import java.lang.Iterable
import scala.collection.JavaConverters.*

/**
 * For each message type, produce the number of the generated log messages
 */
class JobRunner
object JobRunner {
  val logger = CreateLogger(classOf[JobRunner])

  def main(args: Array[String]): Unit  = {
    logger.info("Initializing map reduce job 3")

    // Read the default configuration of the cluster from configuration xml files
    val configuration = new Configuration
    configuration.set("mapred.textoutputformat.separator", ",")
    // Initialize the job with default configuration of the cluster
    val job = Job.getInstance(configuration, "Log Digest")

    // Assign the drive class to the job
    job.setJarByClass(this.getClass)

    // Assign user-defined Mapper and Reducer class
    job.setMapperClass(classOf[JobMapper])
    job.setReducerClass(classOf[JobReducer])

    // Set the Key and Value types of the output
    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[IntWritable])

    // Add input and output path from the args
    FileInputFormat.addInputPath(job, new Path(args(0)))
    FileOutputFormat.setOutputPath(job, new Path(args(1)))
    logger.info("Started map reduce job 3")
    System.exit(if(job.waitForCompletion(true)) 0 else 1)
    
  }

}
