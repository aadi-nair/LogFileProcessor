import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.*
import org.apache.hadoop.io.*
import org.apache.hadoop.util.*
import org.apache.hadoop.mapred.*
import java.io.IOException
import java.util
import LogMessageTypeCount.*
import ErrorLogMessageInterval.*
import LogMessageCharCount.*

import scala.jdk.CollectionConverters.*

object LogFileProcessor:

  @main def runMapReduce(inputPath: String, outputPath: String): RunningJob =
    //    val conf: JobConf = LogMessageTypeCount.getConf(inputPath,outputPath)
    val conf: JobConf = ErrorLogMessageInterval.getConf(inputPath, outputPath)

    JobClient.runJob(conf)