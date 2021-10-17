package Job2

import HelperUtils.CreateLogger
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
    val job = Job.getInstance(configuration, "JOB_ERROR_INTERVALS")

    // Assign the drive class to the job
    job.setJarByClass(this.getClass)

    // Assign user-defined Mapper and Reducer class
    job.setMapperClass(classOf[MapperJob2])
    job.setReducerClass(classOf[ReducerJob2])


    // Set the Key and Value types of the output
    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[IntWritable])

    // Add input and output path from the args
    FileInputFormat.addInputPath(job, new Path(args(0)))
    FileOutputFormat.setOutputPath(job, new Path(args(1)))
    job.waitForCompletion(true)

    // Another job for sorting the results of the previous job

    val configuration2 = new Configuration

    // Initialize the job with default configuration of the cluster
    val job2 = Job.getInstance(configuration2, "JOB_SORT_ERROR_INTERVALS")

    // Assign the drive class to the job
    job2.setJarByClass(this.getClass)

    // Assign user-defined Mapper and Reducer class
    job2.setMapperClass(classOf[MapperJob2_2])
    job2.setReducerClass(classOf[ReducerJob2_2])


    // Set the Key and Value types of the output
    job2.setMapOutputKeyClass(classOf[IntWritable])
    job2.setMapOutputValueClass(classOf[Text])
    job2.setOutputKeyClass(classOf[Text])
    job2.setOutputValueClass(classOf[IntWritable])

    // Add input and output path from the args
    FileInputFormat.addInputPath(job2, new Path(args(1)))
    FileOutputFormat.setOutputPath(job2, new Path(args(2)))

    System.exit(if(job2.waitForCompletion(true)) 0 else 1)

  }

}

