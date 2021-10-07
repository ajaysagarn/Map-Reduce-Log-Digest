package MapReduce

import HelperUtils.CreateLogger

import java.lang.Iterable
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import MapReduce.LogMapper
import MapReduce.LogReducer
import org.apache.hadoop.mapreduce.{Job, Mapper, Reducer}

import scala.collection.JavaConverters.*

class JobRunner
object JobRunner {
  val logger = CreateLogger(classOf[JobRunner])

  def startJob(inputPath: String, outputPath: String): Unit  = {

    logger.info(inputPath)
    logger.info(outputPath)

    // Read the default configuration of the cluster from configuration xml files
    val configuration = new Configuration

    // Initialize the job with default configuration of the cluster
    val job = Job.getInstance(configuration, "Log Digest")

    // Assign the drive class to the job
    job.setJarByClass(this.getClass)

    // Assign user-defined Mapper and Reducer class
    job.setMapperClass(classOf[LogMapper])
    job.setReducerClass(classOf[LogReducer])

    // Set the Key and Value types of the output
    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[IntWritable])

    // Add input and output path from the args
    FileInputFormat.addInputPath(job, new Path(inputPath))
    FileOutputFormat.setOutputPath(job, new Path(outputPath))

    // Exit after completion
    System.exit(if(job.waitForCompletion(true)) 0 else 1)
  }

}
