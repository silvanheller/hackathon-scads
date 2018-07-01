package eu.streamline.hackathon.flink.scala.job.plotting

import eu.streamline.hackathon.flink.scala.job._
import javax.swing.JFrame
import org.jfree.chart.renderer.xy.XYLineAndShapeRenderer
import org.jfree.chart.{ChartFactory, ChartPanel}
import smile.clustering.KMeans

import scala.collection.mutable

/**
  * @author silvan on 01.07.18.
  */
object KMeansVisualizationSingleton {


  val chart = ChartFactory.createScatterPlot("Cluster Assignment for Christianity", "Goldstein", "Average Tone", null)
  val renderer = new XYLineAndShapeRenderer(false, true)
  renderer.setDefaultItemLabelGenerator(new LabelGenerator())
  renderer.setDefaultItemLabelsVisible(true)
  renderer.setAutoPopulateSeriesPaint(true);

  chart.getXYPlot.setRenderer(renderer)
  chart.getXYPlot.getDomainAxis.setRange(-1, 1)
  chart.getXYPlot.getRangeAxis.setRange(-1, 1)

  val panel = new ChartPanel(chart)
  val frame = new JFrame()
  frame.add(panel)
  frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE)
  frame.setLocationRelativeTo(null)

  def update(store: mutable.Map[CountryCode, CountryAggregateData], model: KMeans): Unit = {
    val dataset = new LabeledXYDataset()
    store
      .map(el => (model.predict(Array(el._2.avgGoldstein, el._2.avgAvgTone)), el))
      .groupBy(_._1)
      .foreach(el => {
        el._2.foreach(inner => dataset.add(inner._2._2.avgGoldstein / 10d, inner._2._2.avgAvgTone / 100d, inner._2._1, el._1))
      })

    chart.getXYPlot.setDataset(dataset)
    frame.pack()
    frame.setVisible(true)
  }

}
