package Job1

import HelperUtils.CreateLogger
import Job2.{CustomComparator, MapperJob2, MapperJob2_2, ReducerJob2, ReducerJob2_2}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat


/**
 * Compute a spreadsheet or an CSV file that shows the distribution of different types
 * of messages across predefined time intervals and injected string instances of the
 * designated regex pattern for these log message types
 */
class JobRunner
object JobRunner {
  val logger = CreateLogger(classOf[JobRunner])

  def main(args: Array[String]): Unit  = {
    logger.info("Innitializing map reduce job 1")
    // Read the default configuration of the cluster from configuration xml files
    val configuration = new Configuration
    configuration.set("mapred.textoutputformat.separator", ",")
    // Initialize the job with default configuration of the cluster
    val job = Job.getInstance(configuration, "JOB_LOG_DISTRIBUTION")

    // Assign the drive class to the job
    job.setJarByClass(this.getClass)

    // Assign user-defined Mapper and Reducer class
    job.setMapperClass(classOf[MapperJob1])
    job.setReducerClass(classOf[ReducerJob1])

    // Set the Key and Value types of the output
    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[Text])

    // Add input and output path from the args
    FileInputFormat.addInputPath(job, new Path(args(0)))
    FileOutputFormat.setOutputPath(job, new Path(args(1)))
    logger.info("Map reduce job 1 started")
    System.exit(if(job.waitForCompletion(true)) 0 else 1)
    
  }

}


