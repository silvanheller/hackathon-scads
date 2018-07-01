package eu.streamline.hackathon.flink.scala.job.plotting

import java.awt.{Dimension, GridLayout}

import eu.streamline.hackathon.flink.scala.job.{CountryAggregateData, CountryCode}
import javax.swing.{JFrame, JPanel}
import smile.clustering.KMeans
import smile.plot.{Palette, PlotCanvas, ScatterPlot}

import scala.collection.mutable

/**
  * @author silvan on 01.07.18.
  */
object SmileVisualizationSingleton {

  val frame = new JFrame("Ring Parable")
  frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE)
  frame.setLocationRelativeTo(null)
  frame.setPreferredSize(new Dimension(500, 500))
  val panel = new JPanel(new GridLayout(1, 1))

  var canvas: PlotCanvas = createCanvas()
  panel.add(canvas)
  panel.setPreferredSize(new Dimension(450, 450))
  frame.getContentPane.add(panel)
  frame.pack()
  frame.setVisible(true)

  def update(store: mutable.Map[CountryCode, CountryAggregateData], model: KMeans): Unit = {
    canvas.clear()
    val data = store.values.map(el => Array(el.avgGoldstein, el.avgAvgTone)).toArray
    val labels = store.keys.toArray
    val colors = data.map(d => model.predict(d))
    store
      .map(el => (el._1, Array(el._2.avgGoldstein, el._2.avgAvgTone)))
      .map(el => (el._1, el._2, model.predict(el._2)))
      .groupBy(_._3)
      //.foreach(el => canvas.points)
    //canvas.points(data, labels)
    val temp = ScatterPlot.plot(data, model.getClusterLabel, '.', Palette.COLORS)
  }

  def createCanvas(): PlotCanvas = {
    val res = ScatterPlot.plot(Array(Array(0d, 0d)), Array("Init"))
    res.setTitle("Clustering of Christian Countries")
    res.setAxisLabel(0, "Average Average Tone")
    res.setAxisLabel(1, "Average Goldstein")
  }

}
