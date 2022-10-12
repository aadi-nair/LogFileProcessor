# Homework 1 (Aditya Nair - anair38)

Implementation of a distributed processing of a log file using Map/Reduce.

## Functionality
Homework 1 is divided into four tasks

### Task 1
Compute a CSV file that shows different types of messages across predefined time interval and matching designated regex pattern.

**LogMessageTypeInterval** \
The mapper for each task mentioned from here on, parses the input by splitting the string input by white space, into the following elements. 
- timestamp: string at the 0th index
- log message type( ERROR, INFO, ..): string at the 2nd index
- message: all string after the occurrence of  " - ".

These parsed items are then used to check for pattern matches and interval constraints. Log messages that pass these checks are then put as intermediate keys in the following format:
```
<log-message-type, 1>
Eg:-
<ERROR,1>
<DEBUG,1>
```

The reducer collects all intermediate keys and does an aggregate sum of all the values, outputting the true count.


### Task 2
Compute time intervals in descending order of the frequency of the log type ERROR messages while matching a designated regex pattern.
This task has been done using two Map/Reduce Jobs,
- **ErrorLogMessageInterval**: parses and applies constraints to the input and looks for ERROR messages. Once found the timestamp of the current log message is used to determine what interval it falls into (e.g 2nd 5 sec interval...). The mapper then outputs intermediate key in the following form:
    ```
    <startTimeOfCurrentInterval, 1>
    Eg:- Assuming a 10 sec time interval
    <18:26:00, 1> (original timestamp was 18:26:03.234) 
    <18:26:00, 1> (original timestamp was 18:26:07.234) 
    ```
  The reducer will then receive all ERROR messages that lie in a given interval, since they'll all share the same key i.e start time if the interval they belong to. The reducer will then sum all the occurrences and output in the following format.
  ```
    <startTimeOfCurrentInterval - endTimeOfCurrentInterval, count>
    Eg:- for the above example
    <18:26:00 - 18:26:10, 2>
    ```
- **LogMessageReOrder**: The output from the previous job is read into the mapper, that does the job of switching the key and value. The output would look like
  ```
    <count, startTimeOfCurrentInterval - endTimeOfCurrentInterval>
    <2, 18:26:00 - 18:26:10>
    ```
  a custom comparator class (DescendingKeyComparator) is set as OutputKeyComparatorClass for the job. This sorts the intermediate keys (now the count of ERROR messages) in descending order before being redirected too the reducer.
  The reducer outputs the intermediate keys as is to an output file.
  
  
### Task 3
Produce number of generated log message for each message type.

**LogMessageTypeCount**\
This task is similar to **Task 1** but without time intervals or pattern matching.


### Task 4
Produce number of characters for each log message while matching a designated regex pattern.

**LogMessageCharCount**\
Mapper parses the input and outputs intermediate key,values in the following format\

```
<timestamp, charCount> //since timestamp is unique to each log
```
The reducer outputs the intermediate keys as is.

## Demo
https://youtu.be/kR_X0tVNBJU