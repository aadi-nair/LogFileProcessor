import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.*
import org.apache.hadoop.io.*
import org.apache.hadoop.util.*
import org.apache.hadoop.mapred.*
import java.io.IOException
import java.util
import scala.jdk.CollectionConverters.*
import com.typesafe.config.{Config, ConfigFactory}
import org.slf4j.{Logger, LoggerFactory}
import HelperUtils.CreateLogger


//Finally, you will produce the number of characters in each log message
//for each log message type that contain the highest number of characters
//in the detected instances of the designated regex pattern.
object LogMessageCharCount:
  val logger: Logger = CreateLogger(classOf[LogMessageCharCount.type])

  val applicationConf: Config = ConfigFactory.load("application.conf")

  class Map extends MapReduceBase with Mapper[LongWritable, Text, Text, IntWritable] :
    private final val charCount = new IntWritable(1)
    private val timestamp = new Text()

    @throws[IOException]
    def map(key: LongWritable, value: Text, output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =

      val line: String = value.toString
      // considering log message as all text that occurs after "-"
      try{
        val indexOfDash: Int = line.indexOf(" - ")
        val logMessage: String = line.substring(indexOfDash + 2)
        val lineParts: Array[String] = line.split(" ")
        val pattern: String = applicationConf.getString("logFileProcessor.pattern")
        //checking for pattern`
        if(logMessage.toLowerCase().contains(pattern.toLowerCase())){
          timestamp.set(lineParts(0))
          charCount.set(logMessage.length())
          output.collect(timestamp, charCount)
        }
      }
      catch{
        case outOfIndex: java.lang.IndexOutOfBoundsException =>{
          logger.error(outOfIndex.toString)
          throw outOfIndex
        }
        case configExc: com.typesafe.config.ConfigException => {
          logger.error(configExc.toString)
          throw configExc
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

