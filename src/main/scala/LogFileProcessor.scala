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
import LogMessageReOrder.*
import HelperUtils.CreateLogger
import scala.jdk.CollectionConverters.*
import org.slf4j.{Logger, LoggerFactory}

object LogFileProcessor:
  val logger: Logger = CreateLogger(classOf[LogFileProcessor.type])

  @main def runMapReduce(inputPath: String, outputFolder: String): RunningJob =

    val conf1: JobConf = LogMessageTypeInterval.getConf(inputPath,outputFolder+"output1/")
    logger.info("Job 1 starting")
    val job1: RunningJob = JobClient.runJob(conf1)

    logger.info("Job 2 starting")
    val op2_1: String = outputFolder+"output2_1/"
    val conf2: JobConf = ErrorLogMessageInterval.getConf(inputPath, op2_1)
    val job2: RunningJob = JobClient.runJob(conf2)
    job2.waitForCompletion()
//    outputPath2.split("/")
    val conf2_1: JobConf = LogMessageReOrder.getConf(op2_1, outputFolder+"output2/")
    JobClient.runJob(conf2_1)

    logger.info("Job 3 starting")
    val conf3: JobConf = LogMessageTypeCount.getConf(inputPath,outputFolder+"output3/")
    JobClient.runJob(conf3)

    logger.info("Job 4 starting")
    val conf4: JobConf = LogMessageCharCount.getConf(inputPath,outputFolder+"output4/")
    JobClient.runJob(conf4)


