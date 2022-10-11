import org.apache.hadoop.io.{IntWritable, Text}
import org.mockito.Mockito.{doThrow, verify, verifyNoMoreInteractions, when}
import org.scalatest.funspec.AnyFunSpec
import org.scalatestplus.mockito.MockitoSugar

import scala.jdk.CollectionConverters.IterableHasAsJava
import LogMessageCharCount.*
import ErrorLogMessageInterval.*
import LogMessageTypeCount.*
import org.apache.hadoop.mapred.{OutputCollector, Reporter}

import java.io.IOException


class LogMessageTest extends AnyFunSpec with MockitoSugar:

  private final val ERROR_LOG = "18:48:54.215 [scala-execution-context-global-94] ERROR HelperUtils.Parameters$ - ihu}!A2]*07}|,lc"
  private final val FAULTY_LOG = "18:48:54 [scala-execution-context-global-94] ERROR HelperUtils.Parameters$ - ihu}!A2]*07}|,lc"

  private final val WARN_LOG = "18:49:03.457 [scala-execution-context-global-94] WARN  HelperUtils.Parameters$ - Swq;g+6M:?820=Gmd#.p)sFaqoKc^mm](.A8Z-4-].tp|cfR\\~rc(m^{"

  private final val reporter = mock[Reporter]


  describe("LogMessageCharCount.Map produces intermediate keys <timestamp, charCount>"){
    val mapper = new LogMessageCharCount.Map
    val context = mock[OutputCollector[Text, IntWritable]]
    val timestamp = ERROR_LOG.split(" ")(0)
    mapper.map(key = null, value = new Text(ERROR_LOG), context, reporter)
    verify(context).collect(new Text(timestamp), new IntWritable(17))
  }

  describe("ErrorLogMessageInterval.Map outputs <currentStartTimeInterval,1> when it sees a ERROR message"){
    val mapper = new ErrorLogMessageInterval.Map
    val collector = mock[OutputCollector[Text, IntWritable]]
    mapper.map(key = null, value = new Text(ERROR_LOG), collector, reporter)
    verify(collector).collect(new Text("18:48:46"), new IntWritable(1))
  }

  describe("ErrorLogMessageInterval.Reduce should output <currentStartTimeInterval - endTimeInterval,count>") {
    val values: java.util.List[IntWritable] = java.util.ArrayList()
    values.add(new IntWritable(1))
    values.add(new IntWritable(1))

    val reducer = new ErrorLogMessageInterval.Reduce
    val collector = mock[OutputCollector[Text, IntWritable]]
    reducer.reduce(key = Text("18:48:46"), values = values.iterator(), collector, reporter)
    verify(collector).collect(new Text("18:48:46-18:49:06"), new IntWritable(2))
  }

  describe("mapper should be able to parse message type from log message"){
    val mapper = new LogMessageTypeCount.Map
    val collector = mock[OutputCollector[Text, IntWritable]]

    mapper.map(key = null, value = new Text(WARN_LOG), collector, reporter)
    verify(collector).collect(new Text("WARN"), new IntWritable(1))

  }

  describe("Mapper should throw error when log message is faulty") {
    val mapper = new LogMessageTypeCount.Map
    val collector = mock[OutputCollector[Text, IntWritable]]
    when(mapper.map(key = null, value = new Text(FAULTY_LOG), collector, reporter)).thenThrow((IOException()))
  }

