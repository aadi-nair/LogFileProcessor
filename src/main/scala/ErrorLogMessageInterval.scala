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



//Second, you will compute time intervals sorted in the descending order
//that contained most log messages of the type ERROR with injected regex pattern string instances.
//Please note that it is ok to detect instances of the designated regex pattern that were
//not specifically injected in log messages because they can also be randomly generated.
object ErrorLogMessageInterval:
  val applicationConf: Config = ConfigFactory.load("application.conf")
  val format = new java.text.SimpleDateFormat("hh:mm:ss.SSS")
  val dateFormat: SimpleDateFormat = new SimpleDateFormat("HH:mm:ss")

  def timeDiff(date1: java.util.Date, date2: java.util.Date, timeUnit: java.util.concurrent.TimeUnit): Long =
    val diffInMillies: Long = date2.getTime - date1.getTime
    timeUnit.convert(diffInMillies, java.util.concurrent.TimeUnit.MILLISECONDS);


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
        val startTime = format.parse(applicationConf.getString("logFileProcessor.startTime"))
        val endTime = format.parse(applicationConf.getString("logFileProcessor.endTime"))
        val pattern = applicationConf.getString("logFileProcessor.pattern")
        val interval = applicationConf.getInt("logFileProcessor.interval")


        val line = value.toString
        val lineParts = line.split(" ")

        val indexOfDash = line.indexOf(" - ")
        val logMessage = line.substring(indexOfDash + 2)

        val currentTime = format.parse(lineParts(0))




        if (currentTime.after(startTime) && currentTime.before(endTime) && lineParts(2) == "ERROR" && logMessage.contains(pattern)) {


          val timeDiffValue: Long = timeDiff(startTime, currentTime, java.util.concurrent.TimeUnit.SECONDS)
//          print(currentTime.toString+" "+timeDiffValue+" ")
          val startTimeOfCurrentInterval: java.util.Date = addSeconds(startTime, (interval * (timeDiffValue / interval)).intValue())
//          println(startTimeOfCurrentInterval)


          startTimeString.set(dateFormat.format(startTimeOfCurrentInterval))
          output.collect(startTimeString, one)
        }
      }
      catch {
        case parse: java.text.ParseException => {
          println(parse)
          throw parse
        }
        case configExc: com.typesafe.config.ConfigException => {
          println(configExc)
          throw configExc
        }
        case outOfBound: java.lang.IndexOutOfBoundsException => {
          println(outOfBound)
          throw outOfBound
        }

      }


  class Reduce extends MapReduceBase with Reducer[Text, IntWritable, Text, IntWritable] :
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
        println(parse)
        throw parse
      }
    }


  def getConf(inputPath: String, outputPath: String): JobConf =
    val conf: JobConf = new JobConf(this.getClass)
    conf.setJobName(this.getClass.toString)
    conf.set("fs.defaultFS", "local")
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


