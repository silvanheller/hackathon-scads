package eu.streamline.hackathon.flink.scala.job

import org.apache.flink.streaming.api.functions.windowing.WindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.JavaConverters._


/**
  * Terrible naming but it's a hackathon.
  *
  * This function is responsible for aggregation over a keyed stream of [[GDELTEventWrapper]]s.
  *
  * It additionally receives information about the associated [[TimeWindow]].
  * Since we require this information, using WindowedStream.aggregate is not enough.
  *
  * @author silvan on 01.07.18.
  */
class MyWindowFunction(windowSizeInDays: Int) extends WindowFunction[GDELTEventWrapper, WindowResult, String, TimeWindow] {

  /**
    * Simple avg aggregation over goldstein & avgTone. Additionally percentage-based aggregation over quadclass.
    */
  override def apply(key: String, w: TimeWindow, iterable: java.lang.Iterable[GDELTEventWrapper], collector: Collector[WindowResult]): Unit = {
    /**
      * Initalization
      */
    var country: CountryCode = ""
    var religionPrefix: ReligionPrefix = ""
    var actorNumber: Int = -1
    var count: Int = 0
    var sumGoldstein: Double = 0
    var sumAvgTone: Double = 0
    var sumQuadClass1: Double = 0
    var sumQuadClass2: Double = 0
    var sumQuadClass3: Double = 0
    var sumQuadClass4: Double = 0
    iterable.asScala.foreach(wrapper => {
      if (count == 0) {
        /**
          * Initalize values at first item
          */
        country = wrapper.country
        religionPrefix = wrapper.religionPrefix
        actorNumber = wrapper.actorNumber
      }
      count += 1
      sumAvgTone += wrapper.gdeltEvent.avgTone
      sumGoldstein += wrapper.gdeltEvent.goldstein
      wrapper.gdeltEvent.quadClass.intValue() match {
        case 1 => sumQuadClass1 += 1
        case 2 => sumQuadClass2 += 1
        case 3 => sumQuadClass3 += 1
        case 4 => sumQuadClass4 += 1
        case _ => throw new RuntimeException()
      }
    })
    /**
      * Compute result
      */
    val res = WindowResult(country, religionPrefix, actorNumber,
      count,
      sumGoldstein / count,
      sumAvgTone / count,
      sumQuadClass1.toDouble / count.toDouble,
      sumQuadClass2.toDouble / count.toDouble,
      sumQuadClass3.toDouble / count.toDouble,
      sumQuadClass4.toDouble / count.toDouble,
      //1486080000000L has been manually identified as the earliest point in the dataset
      (w.getStart - 1486080000000L) / (1000 * 60 * 60 * 24 * windowSizeInDays),
      w.getStart)

    /**
      * Hand-off result to collector
      */
    collector.collect(res)
  }

}

/**
  * Simple case class for the new type the transformed stream output
  */
case class WindowResult(var country: CountryCode, var religionPrefix: ReligionPrefix, var actorNumber: ActorNumber, var count: Int, var avgGoldstein: Double, var avgAvgTone: Double, var quadClass1Percentage: Double, var quadClass2Percentage: Double, var quadClass3Percentage: Double, var quadClass4Percentage: Double, var windowIndex: Long, var windowStart: Long)