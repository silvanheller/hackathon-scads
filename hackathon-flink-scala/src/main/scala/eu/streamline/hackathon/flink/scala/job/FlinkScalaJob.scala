package eu.streamline.hackathon.flink.scala.job

import java.io.File
import java.util.Date

import eu.streamline.hackathon.common.data.GDELTEvent
import eu.streamline.hackathon.flink.operations.GDELTInputFormat
import org.apache.commons.io.FileUtils
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

import scala.collection.JavaConverters._

/**
  * Usage: *.jar --path /path/to/dataset
  *
  * Things which are excluded from the analysis:
  * Religion2Code is ignored
  * Subclusters of Religion1Code is ignored
  * Event location is ignored
  */
object FlinkScalaJob {

  def main(args: Array[String]): Unit = {

    val exportHeader = true

    val parameters = ParameterTool.fromArgs(args)
    val pathToGDELT = parameters.get("path")

    /**
      * Boilerplate setup code
      */
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    implicit val typeInfo: TypeInformation[GDELTEvent] = createTypeInformation[GDELTEvent]
    implicit val dateInfo: TypeInformation[Date] = createTypeInformation[Date]

    val source = env
      .readFile[GDELTEvent](new GDELTInputFormat(new Path(pathToGDELT)), pathToGDELT)
      .setParallelism(1)

    /**
      * Pre-filter the stream to avoid [[NullPointerException]]s on Goldstein, avgTone and QuadClass since these are metrics we analyze for all streams.
      */
    val filteredStream: DataStream[GDELTEvent] = source
      .filter((event: GDELTEvent) => {
        event.goldstein != null &&
          event.avgTone != null &&
          event.quadClass != null
      })
      //Assign Timestamps as provided by the boilerplate-code
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[GDELTEvent](Time.seconds(0)) {
      override def extractTimestamp(element: GDELTEvent): Long = {
        element.dateAdded.getTime
      }
    })

    /**
      * We are both interested in Actor 1 and Actor 2.
      * The hacky way to parallelize this is to create two different streams which are then keyed by "CCRRR" where CC is the two-letter country code and RRR the three-letter top-level religion-prefix as defined in the GDELT Codebook.
      * Additionally, we transform the event to our [[GDELTEventWrapper]] to carry over information about the selected actor number for further processing.
      */
    val keyed1Stream = filteredStream
      .filter(event => event.actor1Geo_countryCode != null
        && event.actor1Code_religion1Code != null) //Prevent NPE
      .map(event => GDELTEventWrapper(event, event.actor1Geo_countryCode, event.actor1Code_religion1Code.substring(0, 3), actorNumber = 1))
      .keyBy(wrapper => wrapper.country + wrapper.religionPrefix) //Introduce Partitioning

    val keyed2Stream = filteredStream
      .filter(event => event.actor2Geo_countryCode != null
        && event.actor2Code_religion1Code != null)
      .map(event => GDELTEventWrapper(event, event.actor2Geo_countryCode, event.actor2Code_religion1Code.substring(0, 3), actorNumber = 2))
      .keyBy(wrapper => wrapper.country + wrapper.religionPrefix)

    /**
      * The two keyed streams can be used for global aggregation and window aggregation.
      * Global aggregation is performed over a 200-day window since we know the dataset length is only 180 days.
      */

    /**
      * First, global aggregation for actors 1 and two.
      * Aggregation is performed by applying the [[MyWindowFunction]]
      */
    val aggregatedGlobal1Stream: DataStream[WindowResult] = keyed1Stream
      .window(TumblingEventTimeWindows.of(Time.days(200)))
      .apply((key, win, it, coll) => new MyWindowFunction(200).apply(key, win, it.asJava, coll))

    val aggregatedGlobal2Stream: DataStream[WindowResult] = keyed2Stream
      .window(TumblingEventTimeWindows.of(Time.days(200)))
      .apply((key, win, it, coll) => new MyWindowFunction(200).apply(key, win, it.asJava, coll))

    val windowSizeInDays = 10

    /**
      * Then, windowed aggregation for actors one and two. Again done by applying the [[MyWindowFunction]]
      *
      * Be aware that the .asJava conversion is needed because the [[MyWindowFunction]] interface requires a [[java.lang.Iterable]]...
      */
    val aggregatedWindow1Stream: DataStream[WindowResult] = keyed1Stream
      .window(TumblingEventTimeWindows.of(Time.days(windowSizeInDays)))
      .apply((key, win, it, coll) => new MyWindowFunction(windowSizeInDays).apply(key, win, it.asJava, coll))

    val aggregatedWindow2Stream: DataStream[WindowResult] = keyed2Stream
      .window(TumblingEventTimeWindows.of(Time.days(windowSizeInDays)))
      .apply((key, win, it, coll) => new MyWindowFunction(windowSizeInDays).apply(key, win, it.asJava, coll))

    /**
      * At this point, we have four data streams - a global and windowed one for both actors one and two.
      * Now, we simply export those to .csv-files
      */


    /**
      * Write headers for the csv if the flag is set
      */
    val globalFile = new File("storage/export_global.csv")
    globalFile.delete()
    if (exportHeader) {
      val header = "country,religionPrefix,actorNumber,count,avgGoldstein,avgAvgTone,quadClass1Percentage,quadClass2Percentage,quadClass3Percentage,quadClass4Percentage,windowIndex,windowStart\n"
      FileUtils.writeStringToFile(globalFile, header, true)

      // Headers for the individual days
      (0 to 200 / windowSizeInDays).foreach(idx => {
        val file = new File(s"storage/export_$idx.csv")
        file.delete()
        FileUtils.writeStringToFile(file, header, true)
      })
    }

    /**
      * Global sinks
      */
    aggregatedGlobal1Stream.addSink(res => FileUtils.writeStringToFile(globalFile, res.productIterator.mkString(",") + "\n", true))
    aggregatedGlobal2Stream.addSink(res => FileUtils.writeStringToFile(globalFile, res.productIterator.mkString(",") + "\n", true))

    /**
      * Windowed sinks - write to files which are identified by the windowIndex of the stream-output
      */
    aggregatedWindow1Stream.addSink(res => {
      val file = new File(s"storage/export_${res.windowIndex}.csv")
      FileUtils.writeStringToFile(file, res.productIterator.mkString(",") + "\n", true)
    })
    aggregatedWindow2Stream.addSink(res => {
      val file = new File(s"storage/export_${res.windowIndex}.csv")
      FileUtils.writeStringToFile(file, res.productIterator.mkString(",") + "\n", true)
    })

    env.execute("Flink Scala GDELT Analyzer")

  }

}

/**
  * Simplifies event-processing by carrying the actor-number which this particular stream is interested in.
  */
case class GDELTEventWrapper(gdeltEvent: GDELTEvent, country: CountryCode, religionPrefix: ReligionPrefix, actorNumber: ActorNumber)