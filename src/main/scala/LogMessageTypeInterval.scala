import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.*
import org.apache.hadoop.io.*
import org.apache.hadoop.util.*
import org.apache.hadoop.mapred.*

import java.util
import scala.jdk.CollectionConverters.*
import com.typesafe.config.{Config, ConfigFactory}
import org.slf4j.Logger
import HelperUtils.CreateLogger


//First, you will compute a spreadsheet or an CSV file
//that shows the distribution of different types of messages
//across predefined time intervals and injected string instances of the designated regex pattern
//for these log message types.

object LogMessageTypeInterval :
  val logger: Logger = CreateLogger(classOf[LogMessageTypeInterval.type])
  val applicationConf: Config = ConfigFactory.load("application.conf")
  class Map extends MapReduceBase with Mapper[LongWritable, Text, Text, IntWritable] :
    private final val one = new IntWritable(1)
    private val error = new Text()

    def map(key: LongWritable, value: Text, output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =

      val format = new java.text.SimpleDateFormat("hh:mm:ss.SSS")

      try {
        val startTime: java.util.Date = format.parse(applicationConf.getString("logFileProcessor.startTime"))
        val endTime: java.util.Date = format.parse(applicationConf.getString("logFileProcessor.endTime"))
        val pattern: String = applicationConf.getString("logFileProcessor.pattern")

        val line: String = value.toString
        val lineParts: Array[String] = line.split(" ")
        val currentTime: java.util.Date = format.parse(lineParts(0))

        //extracting log message i.e text after "- "
        val indexOfDash: Int = line.indexOf(" - ")
        val logMessage: String = line.substring(indexOfDash + 2)

        if (currentTime.after(startTime) && currentTime.before(endTime) && logMessage.toLowerCase().contains(pattern.toLowerCase())) {
          error.set(lineParts(2))
          //storing intermediate keys as (<error-type>, one)
          output.collect(error, one)
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
        case outOfBound:java.lang.IndexOutOfBoundsException =>{
          logger.error(outOfBound.toString)
          throw outOfBound
        }
      }




  class Reduce extends MapReduceBase with Reducer[Text, IntWritable, Text, IntWritable] :
    override def reduce(key: Text, values: util.Iterator[IntWritable], output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =
      val sum = values.asScala.reduce((valueOne, valueTwo) => new IntWritable(valueOne.get() + valueTwo.get()))
      output.collect(key, new IntWritable(sum.get()))


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

