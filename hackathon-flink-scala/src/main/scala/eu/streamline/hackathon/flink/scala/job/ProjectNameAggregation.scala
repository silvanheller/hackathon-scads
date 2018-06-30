package eu.streamline.hackathon.flink.scala.job

import org.apache.flink.api.common.functions.AggregateFunction

/**
  * @author silvan on 30.06.18.
  */
class ProjectNameAggregation extends AggregateFunction[GDELTEventWrapper, Accumulator, AccumulatorResult] {
  override def createAccumulator(): Accumulator = {
    Accumulator("", "", 0, 0, 0, 0, 0, 0, 0, 0)
  }

  override def add(wrapper: GDELTEventWrapper, acc: Accumulator): Unit = {
    if (acc.count > 40 && acc.count % 20 == 0) {
      //System.out.println(acc)
    }
    if (acc.count != 0) {
      if (wrapper.country != acc.country || wrapper.religionPrefix != acc.religionPrefix || wrapper.actorNumber != acc.actorNumber) {
        throw new RuntimeException(wrapper.toString + " | " + acc.toString)
      }
    } else {
      acc.country = wrapper.country
      acc.religionPrefix = wrapper.religionPrefix
      acc.actorNumber = wrapper.actorNumber
    }
    acc.count += 1
    acc.sumAvgTone += wrapper.gDELTEvent.avgTone
    acc.sumGoldstein += wrapper.gDELTEvent.goldstein
    wrapper.gDELTEvent.quadClass.intValue() match {
      case 1 => acc.sumQuadClass1 += 1
      case 2 => acc.sumQuadClass2 += 1
      case 3 => acc.sumQuadClass3 += 1
      case 4 => acc.sumQuadClass4 += 1
      case _ => throw new RuntimeException()
    }
  }

  override def getResult(acc: Accumulator): AccumulatorResult = {
    AccumulatorResult(acc.country, acc.religionPrefix, acc.actorNumber,
      acc.count,
      acc.sumGoldstein / acc.count,
      acc.sumAvgTone / acc.count,
      acc.sumQuadClass1,
      acc.sumQuadClass2,
      acc.sumQuadClass3,
      acc.sumQuadClass4)
  }

  override def merge(acc1: Accumulator, acc2: Accumulator): Accumulator = {
    var country = ""
    var religionPrefix = ""
    var actorNumber = 0
    if (acc1.actorNumber == 0 && acc2.actorNumber != 0) {
      country = acc2.country
      religionPrefix = acc2.religionPrefix
      actorNumber = acc2.actorNumber
    }
    if (acc1.actorNumber != 0 && acc2.actorNumber == 0) {
      country = acc1.country
      religionPrefix = acc1.religionPrefix
      actorNumber = acc1.actorNumber
    }
    if (acc1.actorNumber != 0 && acc2.actorNumber != 0) {
      if (acc1.country != acc2.country || acc1.religionPrefix != acc2.religionPrefix || acc1.actorNumber != acc2.actorNumber) {
        throw new RuntimeException(acc1.toString + " | " + acc2.toString)
      }
      country = acc1.country
      religionPrefix = acc1.religionPrefix
      actorNumber = acc1.actorNumber
    }
    Accumulator(country, religionPrefix, actorNumber, acc1.count + acc2.count, acc1.sumGoldstein + acc2.sumGoldstein, acc1.sumAvgTone + acc2.sumAvgTone, acc1.sumQuadClass1 + acc2.sumQuadClass1, acc1.sumQuadClass2 + acc2.sumQuadClass2, acc1.sumQuadClass3 + acc2.sumQuadClass3, acc1.sumQuadClass4 + acc2.sumQuadClass4)
  }
}

case class Accumulator(var country: String, var religionPrefix: String, var actorNumber: Int, var count: Int, var sumGoldstein: Double, var sumAvgTone: Double, var sumQuadClass1: Int, var sumQuadClass2: Int, var sumQuadClass3: Int, var sumQuadClass4: Int)

case class AccumulatorResult(var country: String, var religionPrefix: String, var actorNumber: Int, var count: Int, var avgGoldstein: Double, var avgAvgTone: Double, var sumQuadClass1: Int, var sumQuadClass2: Int, var sumQuadClass3: Int, var sumQuadClass4: Int)