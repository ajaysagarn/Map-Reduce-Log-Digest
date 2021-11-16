## An Implementation of the Map Reduce model to digest logs files generated using a log generator and produce metrics based in the distribution of logs. This project makes use of the HDFS and the map reduce framework to perform required operations on the Log Files. 

## Running this project

This project consists of multiple map Reduce jobs that can be executed within a Hadoop distributed file system.
Below are the steps that need to be followed in order to execute the Map Reduce jobs.
 1. Open the command prompt/terminal at the root directory of the project and run the command ``sbt clean compile assembly``
 2. If you need to execute test cases then run ``sbt clean compile test``
 3. Once the assembly step is completed, a fat jar file will be generated under ``target/scala-3.0.2/<Jar name>``
 4. In order to generate an input log file, from within the terminal run ``sbt clean compile run`` and then select the LogGenerator class. This will generate a log file according to the parameters set in the ``application.conf`` file.

#### Executing in Horton's sandbox
Here we use the Horton's hadoop sandbox to execute our map reduce jobs.
1. Use a scp tool, transfer the jar file as well as the generate log file into the hortons VM instance.
2. Move the input file into a hdfs directory using the command ``hdfs dfs -put <name of the log file> /hdfs directory``
3. Run the individual jobs from the jar file by running the command ``hadoop jar <classname> /<input directory path> /<output directory path>``
4. Once the job executes successfully, the output can be viewed at the specified output folder

#### Executing in AWS EMR Cluster
Here we execute our map reduce jobs on the AWS EMR Custer
1. Create an S3 bucket and move the Fat jar as well as the generated log files into separate folders within the S3 bucket.
2. From the AWS console, navigate to the EMR service and click on create cluster.
3. Here we create a cluster to execute step jobs and create multiple Custom Jar execution steps for each Map reduce jobs that we need to run.
4. For each of the steps created this way, we need to specify the s3 location of the jar and provide the appropriate arguments to the job.
5. Finally click on create cluster. This will start the resource provisioning process and execute the steps one after the other.
6. The output files can be viewed in the s3 path specified in the arguments passed for each of the jobs. 


[Implementation Details](./docs/Implementation.md)
