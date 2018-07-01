package eu.streamline.hackathon.flink.scala.job

import java.util.concurrent.atomic.AtomicInteger

import eu.streamline.hackathon.common.data.GDELTEvent
import eu.streamline.hackathon.flink.scala.job.plotting.KMeansVisualizationSingleton
import org.apache.commons.math3.ml.distance.EuclideanDistance
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import smile.clustering.KMeans

import scala.collection.mutable

/**
  * @author silvan on 30.06.18.
  */

case class CountryAggregateData(numObservations: Counter, goldsteinSum: GoldsteinSum, avgToneSum: avgToneSum) {
  def avgGoldstein: Double = goldsteinSum / numObservations

  def avgAvgTone: Double = avgToneSum / numObservations
}

class KMeansSink(targetK: Int) extends SinkFunction[GDELTEvent] with Serializable {

  var counter = new AtomicInteger(0)
  val store: mutable.Map[CountryCode, CountryAggregateData] = new mutable.HashMap()

  override def invoke(event: GDELTEvent): Unit = {
    counter.incrementAndGet()
    val key = event.actor1Geo_countryCode
    if (!store.contains(key)) {
      store.put(key, CountryAggregateData(0, 0d, 0d))
    }
    store.update(key, CountryAggregateData(store(key).numObservations + 1, store(key).goldsteinSum + event.goldstein, store(key).avgToneSum + event.avgTone))

    if (counter.get() == 10) {
      tick()
    }

    if (counter.get() % 1000 == 0) {
      tick()
    }

  }

  def tick(): Unit = {
    System.out.println(s"Tick: ${counter.get()}")
    val data = store.values.map(el => Array(el.avgGoldstein, el.avgAvgTone)
    ).toArray
    val model = KMeans.lloyd(data, targetK)
    //SmileVisualizationSingleton.update(store, model)
    KMeansVisualizationSingleton.update(store, model)
  }
}

class Node(val x: Goldstein, val y: avgTone, val country: CountryCode) {
  def distanceTo(node: Node): Double = {
    new EuclideanDistance().compute(Array(x, y), Array(node.x, node.y))
  }
}

case class Centroid(center: Node, elements: mutable.Map[CountryCode, Node])
