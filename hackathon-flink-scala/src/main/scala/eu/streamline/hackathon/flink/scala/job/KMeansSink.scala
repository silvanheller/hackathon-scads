package eu.streamline.hackathon.flink.scala.job

import eu.streamline.hackathon.common.data.GDELTEvent
import javax.swing.JFrame
import org.apache.commons.math3.ml.distance.EuclideanDistance
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.jfree.chart.renderer.xy.XYLineAndShapeRenderer
import org.jfree.chart.{ChartFactory, ChartPanel}
import smile.clustering.KMeans

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * Online K-Means from https://arxiv.org/pdf/1412.5721.pdf
  *
  * @author silvan on 30.06.18.
  */
class KMeansSink(targetK: Int) extends SinkFunction[GDELTEvent] with Serializable {
  val chart = ChartFactory.createScatterPlot("Cluster Assignment for Christianity", "Goldstein", "Average Tone", null)
  val renderer = new XYLineAndShapeRenderer(false, true)
  renderer.setDefaultItemLabelGenerator(new LabelGenerator())
  chart.getXYPlot.setRenderer(renderer)
  val panel = new ChartPanel(chart)
  val frame = new JFrame()
  frame.add(panel)
  frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE)
  frame.setLocationRelativeTo(null)
  var counter = 0
  val store: mutable.Map[CountryCode, (Counter, GoldsteinSum, avgToneSum)] = new mutable.HashMap()
  val centroids: ListBuffer[Centroid] = new ListBuffer()
  var model: KMeans = _

  override def invoke(event: GDELTEvent): Unit = {
    counter += 1
    val key = event.actor1Geo_countryCode
    if (!store.contains(key)) {
      store.put(key, (0, 0d, 0d))
    }
    store.update(key, (store(key)._1 + 1, store(key)._2 + event.goldstein, store(key)._3 + event.avgTone))

    val node = new Node(store(key)._2 / store(key)._1, store(key)._3 / store(key)._1, event.actor1Geo_countryCode)

    if (counter % 100 == 0) {
      updateCentroids()
      updateVisualization()
    }

  }

  def eventToRel(event: GDELTEvent): String = event.actor1Code_religion1Code.substring(0, 3)

  def updateCentroids(): Unit = {
    val data = store.values.map(el => Array(el._3 / el._1, el._2 / el._1)).toArray
    model = KMeans.lloyd(data, targetK)
  }

  def updateVisualization(): Unit = {
    val dataset = new LabeledXYDataset()
    store.map(el => (model.predict(Array(el._2._3 / el._2._1, el._2._2 / el._2._1)), el)).groupBy(_._1).foreach(el => {
      el._2.foreach(inner => dataset.add(inner._2._2._3 / inner._2._2._1, inner._2._2._2 / inner._2._2._1, inner._2._1, inner._1))
    })

    chart.getXYPlot.setDataset(dataset)
    frame.pack()
    frame.setVisible(true)
  }
}

class Node(val x: Goldstein, val y: avgTone, val country: CountryCode) {
  def distanceTo(node: Node): Double = {
    new EuclideanDistance().compute(Array(x, y), Array(node.x, node.y))
  }
}

case class Centroid(center: Node, elements: mutable.Map[CountryCode, Node])
