import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.*
import org.apache.hadoop.io.*
import org.apache.hadoop.util.*
import org.apache.hadoop.mapred.*

import java.io.IOException
import java.util
import org.slf4j.{Logger, LoggerFactory}
import HelperUtils.CreateLogger

//import DescendingKeyComparator.*

import scala.jdk.CollectionConverters.*


object LogMessageReOrder:
  val logger: Logger = CreateLogger(classOf[LogMessageReOrder.type])
  class DescendingKeyComparator extends WritableComparator(classOf[IntWritable], true) {

    @SuppressWarnings(Array("rawtypes"))
    override def compare(a: WritableComparable[_], b: WritableComparable[_]): Int = {
      val key1: IntWritable = a.asInstanceOf[IntWritable]
      val key2: IntWritable = b.asInstanceOf[IntWritable]
      -1 * key1.compareTo(key2)
    }
  }

  class Map extends MapReduceBase with Mapper[LongWritable, Text, IntWritable, Text] :
    private final val one = new IntWritable(1)
    private val word = new Text()

    @throws[IOException]
    def map(key: LongWritable, value: Text, output: OutputCollector[IntWritable, Text], reporter: Reporter): Unit =
      val line: String = value.toString
      try {
        val lineParts: Array[String] = line.split(",")
        word.set(lineParts(0))
        output.collect(new IntWritable(lineParts(1).toInt), word)
      }
      catch{
        case pattSyntExc: java.util.regex.PatternSyntaxException => {
          logger.error(pattSyntExc.toString)
        }
        case exc: java.lang.NumberFormatException =>{
          logger.error(exc.toString)
        }

      }


  class Reduce extends MapReduceBase with Reducer[IntWritable, Text, IntWritable, Text] :
    override def reduce(key: IntWritable, values: util.Iterator[Text], output: OutputCollector[IntWritable, Text], reporter: Reporter): Unit =
//      val sum = values.asScala.reduce((valueOne, valueTwo) => new IntWritable(valueOne.get() + valueTwo.get()))
      output.collect(key, values.next())


  def getConf(inputPath: String, outputPath: String): JobConf =
//    val config: Configuration = new Configuration()
    val conf: JobConf = new JobConf(this.getClass)
    conf.setJobName(this.getClass.toString)
//    conf.set("fs.defaultFS", "local")
    conf.set("mapreduce.job.maps", "1")
    conf.set("mapreduce.job.reduces", "1")
    conf.set("mapred.textoutputformat.separator", ",")
    conf.setOutputKeyClass(classOf[IntWritable])
    conf.setOutputValueClass(classOf[Text])
    conf.setMapperClass(classOf[Map])
    conf.setCombinerClass(classOf[Reduce])
    conf.setReducerClass(classOf[Reduce])
    conf.setInputFormat(classOf[TextInputFormat])
    conf.setOutputFormat(classOf[TextOutputFormat[IntWritable, Text]])
    conf.setOutputKeyComparatorClass(classOf[DescendingKeyComparator])
    FileInputFormat.setInputPaths(conf, new Path(inputPath))
    FileOutputFormat.setOutputPath(conf, new Path(outputPath))
    conf
