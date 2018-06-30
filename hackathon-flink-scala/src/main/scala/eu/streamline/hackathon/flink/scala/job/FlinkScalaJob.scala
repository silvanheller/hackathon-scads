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

object FlinkScalaJob {

  def main(args: Array[String]): Unit = {

    val ashery = false
    val lukasHeader = false

    val parameters = ParameterTool.fromArgs(args)
    val pathToGDELT = parameters.get("path")
    System.out.println("Path: " + pathToGDELT)

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    implicit val typeInfo: TypeInformation[GDELTEvent] = createTypeInformation[GDELTEvent]
    implicit val dateInfo: TypeInformation[Date] = createTypeInformation[Date]

    val source = env
      .readFile[GDELTEvent](new GDELTInputFormat(new Path(pathToGDELT)), pathToGDELT)
      .setParallelism(1)

    //To Discuss: Religion2Code?

    val filteredStream: DataStream[GDELTEvent] = source
      .filter((event: GDELTEvent) => {
        event.goldstein != null &&
          event.avgTone != null &&
          event.quadClass != null
      })
      //Prevent Nullpointer exceptions*/
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[GDELTEvent](Time.seconds(0)) {
      override def extractTimestamp(element: GDELTEvent): Long = {
        element.dateAdded.getTime
      }
    })
    if (ashery) {
      val file = new File("storage/data.csv")
      val header = "country\treligion\tgoldstein\tavgTone"
      FileUtils.writeStringToFile(file, s"$header\n", true)

      filteredStream.filter(el => el.actor1Geo_countryCode != null && el.actor1Code_religion1Code != null).addSink(el => FileUtils.writeStringToFile(file, s"${el.actor1Geo_countryCode}\t${el.actor1Code_religion1Code.substring(0, 3)}\t${el.goldstein}\t${el.avgTone}\n", true))

    }
    val keyed1Stream = filteredStream
      .filter(event => event.actor1Geo_countryCode != null
        && event.actor1Code_religion1Code != null)
      .map(event => {
        if (event.actor1Geo_countryCode.length != 2) {
          System.err.println("Country code is not length 2: " + event.actor1Geo_countryCode)
        }
        event
      })
      .map(event => GDELTEventWrapper(event, event.actor1Geo_countryCode, event.actor1Code_religion1Code.substring(0, 3), 1))
      .keyBy(wrapper => wrapper.country + wrapper.religionPrefix)

    //TODO keyed2Stream

    val aggregated1Stream: DataStream[AccumulatorResult] = keyed1Stream.window(TumblingEventTimeWindows.of(Time.days(200))).aggregate(new ProjectNameAggregation())

    val file = new File("storage/export.csv")

    file.delete()

    if (lukasHeader) {
      FileUtils.writeStringToFile(file, "country,religionPrefix,actorNumber,count,avgGoldstein,avgAvgTone,sumQuadClass1,sumQuadClass2,sumQuadClass3,sumQuadClass4", true)
    }

    aggregated1Stream.addSink(res => FileUtils.writeStringToFile(file, res.productIterator.mkString(",") + "\n", true))

    env.execute("Flink Scala GDELT Analyzer")

  }

}

case class GDELTEventWrapper(gDELTEvent: GDELTEvent, country: String, religionPrefix: String, actorNumber: Int)