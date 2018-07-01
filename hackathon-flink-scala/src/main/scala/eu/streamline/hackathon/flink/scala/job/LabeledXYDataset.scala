package eu.streamline.hackathon.flink.scala.job

import org.jfree.chart.labels.XYItemLabelGenerator
import org.jfree.data.xy.{AbstractXYDataset, XYDataset}

import scala.collection.mutable.ListBuffer

class LabeledXYDataset extends AbstractXYDataset {
  private val x = new ListBuffer[Number]()
  private val y = new ListBuffer[Number]()
  private val label = new ListBuffer[String]()
  private val series = new ListBuffer[Number]()

  def add(x: Double, y: Double, label: String, series: Int): Unit = {
    this.x += x
    this.y += y
    this.label += label
    this.series += series
  }

  def getLabel(series: Int, item: Int): String = label(item)

  override def getSeriesCount: Counter = series.size

  override def getSeriesKey(series: Int) = s"Cluster $series"

  override def getItemCount(series: Int): Int = label.size

  override def getX(series: Int, item: Int): Number = x(item)

  override def getY(series: Int, item: Int): Number = y(item)
}

class LabelGenerator extends XYItemLabelGenerator {
  override def generateLabel(dataset: XYDataset, series: Int, item: Int): String = {
    val labelSource = dataset.asInstanceOf[LabeledXYDataset]
    labelSource.getLabel(series, item)
  }

}
