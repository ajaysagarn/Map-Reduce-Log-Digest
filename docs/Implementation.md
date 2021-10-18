### Implementation Details

This project consists of several Map Reduce jobs that are created to accept log inputs and produces several statistics 
about the logs. Here we use several parameters specified in the ```application.conf``` file to generate the log messages of the desired characteristics.

Util classes have been written in the ```MapReduceUtils``` class that performs most of the generic operations that are being used in most of the mappers and
reducers.

#### Testing: 
Unit test cases are written to test several methods in the util class to make sure they produce the desired results.

### Time Intervals

Inorder to group log messages into respective time intervals we first specify a global constant in the ```application.conf``` file which specifies the length of each interval.

In this project, based on the value of the time interval specified within the configuration file we assume the entire day i.e 24 hours as divided into time intervals of specified bucket size.

For example:- If the time interval is set to 5 minutes, we divide the day into time interval as follows

    00:00 - 00:05 -> first time interval
    00:05 - 00:10 -> second time interval
    .....
    .....
    23:45 - 23:50 -> another time interval

similarly if the time interval is set to 20 minutes, then the time intervals for the day would be
    
    00:00 - 00:20 -> first time interval
    00:20 - 00:40 -> second time interval
    .....
    .....
    23:30 - 23:50 -> another time interval

Now when we have a new log entry that has a valid timestamp, we fetch the hours and the minutes part of the timestamp and determine the correct time interval to which the
current log message belongs to. It is made sure that all log messages that fall under a given time interval will be given the same time interval key in all the mapper classes that require time intervals to be created.

## Map Reduce Jobs

### Map Reduce Job - 1 :
"Compute a spreadsheet or an CSV file that shows the distribution of different types
of messages across predefined time intervals and injected string instances of the
designated regex pattern for these log message types"

In this map reduce job we perform map reduce on the log file to determine the distribution of each type of log message and also find the number of messages for each of these log types
that matched with the predefined regex pattern.

####Mapper:
In the mapper phase of this job we first determine the time interval for each log input and set this time interval as the key output value for the mapper phase.
For the value we extract the Log Type as well as the Message part of the log and concat them together to be outputted as the value for the mapper.

Example mapper output:-

    <key , value>
    <12:45 - 12:50 , "DEBUG - asjdfhb@#$$jshbdfv##@#%$^jsshdbfkssv">
    <12:50 - 12:55 , "DEBUG - iy182uhdfg@#$$jshbdfv##@#%$^erig!@">
    <12:55 - 13:00 , "WARN - erigr2384uQ#$%@#$$jshbdfv##@#%$^srog5W$#%6">

####Reducer:

In the reducer phase of the job, we create aggregations over each time interval key we get from the mapper. Within each time interval we count the number of messages that are present for each Log type as well as the number of messages for each log type that match the predefined regex pattern.
finally, for each time interval we make 4 context writes, one for each type of log message (WARN, DEBUG, ERROR, INFO).

#### Results : [Map Reduce Job 1 Results](../results/Job1.csv)


### Map Reduce Job - 2 :
"Compute time intervals sorted in the descending order that contained most
log messages of the type ERROR with injected regex pattern string instances."

In this Map Reduce job we create a chain of two map-reduce jobs where the output produced by the first map reduce job is used as the input for the second map reduce job.
This is done in order to be able to sort the results in descending order of number of error messages in a time interval.

####Mapper_1:
In the first mapper phase we simply determine the time interval that a log belongs to and use this time interval as the key output of the mapper.
We then check if the log is of type ERROR and also check if the message has the predefined regex pattern in it. If both the conditions are satisfied, then we simply add 1 as the value output of the mapper

Example mapper_1 output:-

    <key , value>
    <12:45 - 12:50 , 1>
    <12:45 - 12:50 , 1>
    <12:45 - 12:50 , 1>
    <12:50 - 12:55 , 1>
    <12:55 - 13:00 , 1>

####Reducer_1:

In the first reducer phase of the job, we sum up all the individual counts for each time interval to get the required count of the number of error messages that matched the regex pattern for that time interval. Here the output would not have any specific sorting applies to it.

Example reducer_1 output:-

    <key , value>
    <12:45 - 12:50 , 3>
    <12:50 - 12:55 , 15>
    <12:55 - 13:00 , 12>

####Mapper_2:
In the second mapper phase, the output of the first map reduce job is taken as the input. Here we simply switch the key and the values that we get from the first Map-reduce job. This is done because, when we have the count(i,e the count of the number of error messages matching the pattern for each time interval) as the key, we can then use the inbuilt ``job.setSortComparatorClass`` function that allows us to set a comparator to sort the mapper output based on its key values.
Here for the second map reduce job we apply a custom comparator ``CustomComparator.scala`` which will sort the mapper keys in descending order.

Example mapper_2 output:-

    <key , value>
    <15, 12:50 - 12:55>
    <12, 12:55 - 13:00>    
    <3, 12:45 - 12:50>
    

####Reducer_2:

Finally in the second reducer phase of the job, we already have the required information in the correct sorted order. Here we simply switch back the key and the value pairs so that the time intervals are the keys and the counts as the values in the final output

Example reducer_2 output:-

    <key , value>
    <12:50 - 12:55 , 15>    
    <12:55 - 13:00 , 12>
    <12:45 - 12:50 , 3>

#### Results : [Map Reduce Job 2 Results](../results/Job2.csv)

### Map Reduce Job - 3 :
For each message type, produce the number of the generated log messages

Here we need to simply generate the number of messages of each log type in out inout file. This is done in the following steps

####Mapper:
In the mapper phase we first check if the log has a valid log level, and if it does we output a key value pair where the logType is the key and 1 as the value.

Example mapper output:-

    <key , value>
    <DEBUG , 1>
    <DEBUG , 1>
    <INFO , 1>
    <DEBUG , 1>
    <DEBUG , 1>
    <INFO , 1>
    <DEBUG , 1>
    <DEBUG , 1>
    <INFO , 1>

####Reducer:
In the reducer phase we aggregate the results by summing up the list of values for each log type to get the final distribution of the number of messages for each log type

#### Results : [Map Reduce Job 3 Results](../results/Job3.csv)

### Map Reduce Job - 4 :
Produce the number of characters in each log message for each log message type
that contain the highest number of characters in the detected instances of the designated regex pattern.

In this job we try to fins the length of the largest message that matches the predefined regex pattern for each log types. This is done in the following map reduce steps
####Mapper:

In the map reduce phase, we first get the log type for the log and use this as the key output value of the mapper.
We then get the message part of the log and set this as the value output of the mapper.

Example mapper output:-

    <key , value>
    <DEBUG , kdfbgfg!@#$@#4978y9475yskdhjfbsdfkgb>
    <DEBUG , SGTKREGHKdkdhrgfbkreg#@$%@^23o458ul34>
    <INFO ,  leigdfgb@#%!@%sdfjhgbg%^&^sddbgsdsfbg>
    <DEBUG , djkfhguh!@%^$^$%^ksb569456>
    <DEBUG , SDGTLERG#$%@#$%!@#%8yksjdfihigerg>
    <INFO ,  AERGOUERHG!@#%#@!%98798ydjfvb>
    <DEBUG , SHGERUGH34985y36@$&$%>
    <DEBUG , GEORGH!@#%#$^08078>
    <INFO ,  EROGUHERO!@#$%#@#^586>

####Reducer:
In the reducer phase of the job we go through the list of all values for each log type and determine the length of the largest string for that log message.
The length of the largest message found in this way is then written as the output for the respective log type
#### Results : [Map Reduce Job 4 Results](../results/Job4.csv)


