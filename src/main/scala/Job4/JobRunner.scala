package Job4

import HelperUtils.CreateLogger
import Job2.{MapperJob2, MapperJob2_2, ReducerJob2, ReducerJob2_2}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat

class JobRunner
object JobRunner {
  val logger = CreateLogger(classOf[JobRunner])

  def main(args: Array[String]): Unit  = {

    // Read the default configuration of the cluster from configuration xml files
    val configuration = new Configuration

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
    System.exit(if(job.waitForCompletion(true)) 0 else 1)

  }

}


