import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.*
import org.apache.hadoop.io.*
import org.apache.hadoop.util.*
import org.apache.hadoop.mapred.*
import java.io.IOException
import java.util
import scala.jdk.CollectionConverters.*
import com.typesafe.config.{Config, ConfigFactory}
import java.util.Calendar
import java.text.DateFormat
import java.text.SimpleDateFormat
import org.slf4j.{Logger, LoggerFactory}
import HelperUtils.CreateLogger




//Second, you will compute time intervals sorted in the descending order
//that contained most log messages of the type ERROR with injected regex pattern string instances.
//Please note that it is ok to detect instances of the designated regex pattern that were
//not specifically injected in log messages because they can also be randomly generated.
object ErrorLogMessageInterval:
  val applicationConf: Config = ConfigFactory.load("application.conf")
  val inputDateFormat = new java.text.SimpleDateFormat("hh:mm:ss.SSS")
  val dateFormat: SimpleDateFormat = new SimpleDateFormat("HH:mm:ss")
  val logger: Logger = CreateLogger(classOf[ErrorLogMessageInterval.type])


  /**
   * fetch difference between two Date variables and return difference in Seconds
   * @param date1
   * @param date2
   * @param timeUnit: time unit of the difference to return i.e seconds, minutes, hours...
   * @return
   */
  def timeDiff(date1: java.util.Date, date2: java.util.Date, timeUnit: java.util.concurrent.TimeUnit): Long =
    val diffInMillies: Long = date2.getTime - date1.getTime
    timeUnit.convert(diffInMillies, java.util.concurrent.TimeUnit.MILLISECONDS);

  /**
   *
   * allows adding seconds to a Date
   * @param date: Date to add seconds to
   * @param seconds: seconds in int to add to Date
   * @return Date with seconds added
   */
  def addSeconds(date: java.util.Date, seconds: Integer): java.util.Date = {
    val cal = Calendar.getInstance
    cal.setTime(date)
    cal.add(Calendar.SECOND, seconds)
    cal.getTime
  }

  class Map extends MapReduceBase with Mapper[LongWritable, Text, Text, IntWritable] :
    private final val one = new IntWritable(1)
    private val startTimeString = new Text()


    @throws[IOException]
    def map(key: LongWritable, value: Text, output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =

      try {
        /**
         * parsing variables from conf file
         * @val startTime: predefined timestamp from where log processing would begin.
         * @val endTime: predefined timestamp from where log processing would end.
         * @val pattern: pattern to look for in log messages
         * @val interval: interval to consider when grouping ERROR messgaes
         */
        val startTime = inputDateFormat.parse(applicationConf.getString("logFileProcessor.startTime"))
        val endTime = inputDateFormat.parse(applicationConf.getString("logFileProcessor.endTime"))
        val pattern = applicationConf.getString("logFileProcessor.pattern")
        val interval = applicationConf.getInt("logFileProcessor.interval")

        val line = value.toString
        val lineParts = line.split(" ")

        // considering log message as all text that occurs after "-"
        val indexOfDash = line.indexOf(" - ")
        val logMessage = line.substring(indexOfDash + 2)
        val currentTime = inputDateFormat.parse(lineParts(0))

        //checking for time-constraints and pattern
        if (currentTime.after(startTime) && currentTime.before(endTime) && lineParts(2) == "ERROR" && logMessage.toLowerCase().contains(pattern.toLowerCase())) {
          /**
           * @val timeDiffValue: calculates time difference between current time and startTime in seconds
           * @val startTimeOfCurrentInterval: calculates the startTime of the interval the current timestamp would belong to.
           * it does so by calculating the timeDiffValue and dividing it by interval to get how many intervals have passed
           *
           */
          val timeDiffValue: Long = timeDiff(startTime, currentTime, java.util.concurrent.TimeUnit.SECONDS)
          val startTimeOfCurrentInterval: java.util.Date = addSeconds(startTime, (interval * (timeDiffValue / interval)).intValue())

          /**
           * storing intermediate keys as (startTime of interval they belong to, count)
           * all log messages withing an interval would be grouped together by their interval-startTime
           */
          startTimeString.set(dateFormat.format(startTimeOfCurrentInterval))
          output.collect(startTimeString, one)
        }
      }
      catch {
        case parse: java.text.ParseException => {
          logger.error(parse.toString)
          throw parse
        }
        case configExc: com.typesafe.config.ConfigException => {
          logger.error(configExc.toString)
          throw configExc
        }
        case outOfBound: java.lang.IndexOutOfBoundsException => {
          logger.error(outOfBound.toString)
          throw outOfBound
        }

      }


  class Reduce extends MapReduceBase with Reducer[Text, IntWritable, Text, IntWritable] :
    /**
     *
     * @param key      : startTime of every interval (predefined)
     * @param values   : list of counts of each error message belonging to that interval
     *
     * parsing key i.e intervalStartTime of current interval to determine the intervalEndTime
     * reducer stores the final result as (intervalStartTime - intervalEndTime, count)
     */
    override def reduce(key: Text, values: util.Iterator[IntWritable], output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =
    try{

      val intervalStartTime: java.util.Date = dateFormat.parse(key.toString)
      val interval = applicationConf.getInt("logFileProcessor.interval")
      val endTimeOfCurrentInterval: java.util.Date = addSeconds(intervalStartTime, interval)

      val sum = values.asScala.reduce((valueOne, valueTwo) => new IntWritable(valueOne.get() + valueTwo.get()))
      output.collect(new Text(dateFormat.format(intervalStartTime) + "-" + dateFormat.format(endTimeOfCurrentInterval)), new IntWritable(sum.get()))
    }
    catch{
      case parse: java.text.ParseException => {
        logger.error(parse.toString)
        throw parse
      }
    }


  def getConf(inputPath: String, outputPath: String): JobConf =
    val conf: JobConf = new JobConf(this.getClass)
    conf.setJobName(this.getClass.toString)
//    conf.set("fs.defaultFS", "local")
    conf.set("mapreduce.job.maps", "1")
    conf.set("mapreduce.job.reduces", "1")
    conf.set("mapred.textoutputformat.separator", ",");
    conf.setOutputKeyClass(classOf[Text])
    conf.setOutputValueClass(classOf[IntWritable])
    conf.setMapperClass(classOf[Map])
    conf.setCombinerClass(classOf[Reduce])
    conf.setReducerClass(classOf[Reduce])
    conf.setInputFormat(classOf[TextInputFormat])
    conf.setOutputFormat(classOf[TextOutputFormat[Text, IntWritable]])
    FileInputFormat.setInputPaths(conf, new Path(inputPath))
    FileOutputFormat.setOutputPath(conf, new Path(outputPath))
    conf


