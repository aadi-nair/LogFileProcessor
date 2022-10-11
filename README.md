# Homework 1 (Aditya Nair - anair38)

Implementation of a distributed processing of a log file using Map/Reduce.

## Functionality
Homework 1 is divided into four tasks

### Task 1
Compute a CSV file that shows different types of messages across predefined time interval and matching designated regex pattern.

#### Implementation
The mapper is tasked with checking for every line it reads from the log message, to parse it into
- timestamp
- log message type( ERROR, INFO, ..)
- message

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
- one to parse and count intervals and the number of ERROR messages in them
- second to read from output file of first job and sort the values in descending order





### Task 3
Produce number of generated log message for each message type.

### Task 4
Produce number of characters for each log message while matching a designated regex pattern.

https://youtu.be/kR_X0tVNBJU