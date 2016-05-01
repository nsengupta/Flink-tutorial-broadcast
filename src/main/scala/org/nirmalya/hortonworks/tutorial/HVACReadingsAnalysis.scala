package org.nirmalya.hortonworks.tutorial

import org.apache.flink.api.common.functions.{BroadcastVariableInitializer, RichMapFunction}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._
import org.apache.flink.api.java.io.CsvReader
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.util.Collector
import org.joda.time.DateTime
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}
import scala.collection.breakOut

import scala.io.Source

import scala.collection.JavaConverters._


/**
 * Created by nirmalya on 27/2/16.
 */

case class HVACData(
     dateOfReading: String, timeOfReading: String ,actualTemp: Int,targetTemp: Int,
     systemID: Int, systemAge: Int, buildingID: Int) {

  override def canEqual(a: Any) = a.isInstanceOf[HVACData]

  override def equals(that: Any): Boolean =

    that match {
      case e: HVACData => e.canEqual(this)            &&
        (this.buildingID == e.buildingID)
      case _ => false
    }

  // '==' and '##' should go together
  // The following logic is taken from 'Thinking in Java'
  override def hashCode:Int = {
    buildingID.hashCode()
  }
}

case class BuildingInformation(buildingID: Int, buildingManager: String, buildingAge: Int, productID: String, country: String)

object UndefinedBuildingInformation extends BuildingInformation(-1,"UnknownManager",-1,"UnknownProduct","UnknownCountry")

case class EnhancedHVACTempReading(buildingID: Int, rangeOfTemp: String, extremeIndicator: Int,country: String, productID: String,buildingAge: Int, buildingManager: String)

object HVACReadingsAnalysis {

  val incomingFormat = DateTimeFormat.forPattern("MM/dd/yy HH:mm:ss")

  def main(args: Array[String]): Unit = {

    val envDefault = ExecutionEnvironment.getExecutionEnvironment
    val buildingsBroadcastSet = prepareBuildingInfoSet(envDefault,"./SensorFiles/building.csv")

    val hvacDataSetFromFile = readHVACReadings(envDefault,"./SensorFiles/HVAC.csv")

    val joinedBuildingHvacReadings = hvacDataSetFromFile
      .map(new HVACToBuildingMapper)
      .withBroadcastSet(buildingsBroadcastSet,"buildingData")


    /*val  extremeTemperaturesRecordedByCountry = joinedBuildingHvacReadings
      .filter(reading => reading.rangeOfTemp == "HOT" || reading.rangeOfTemp == "COLD")
      .groupBy("country")
      .reduceGroup(nextGroup => {
            val asList = nextGroup.toList
        (asList.head.country,asList.size)
      })
      .writeAsCsv("./countrywiseTempRange.csv")*/

    val hvacDevicePerformance =
      joinedBuildingHvacReadings
      .map(reading => (reading.productID,reading.extremeIndicator))
      .filter(e => (e._2 == 1))    // 1 == Extreme Temperature observed
      .groupBy(0)                  // ProductID
      .reduceGroup(nextGroup => {
            val asList = nextGroup.toList
            (asList.head._1,asList.size)
    })
    .writeAsCsv("./hvacDevicePerformance.csv")

    envDefault.execute("HVAC Simulation")

  }

  private def prepareBuildingInfoSet(env: ExecutionEnvironment, inputPath: String): DataSet[BuildingInformation] = {

     val inputDataFromFile =
       Source
        .fromFile(inputPath)
        .getLines
        .drop(1)
        .map(datum => {

           val fields = datum.split(",")

          BuildingInformation(
            fields(0).toInt,     // buildingID
            fields(1),           // buildingManager
            fields(2).toInt,     // buildingAge
            fields(3),           // productID
            fields(4)            // Country
          )
        })

     env.fromCollection(inputDataFromFile.toList)
  }

  private def readHVACReadings(env: ExecutionEnvironment, inputPath: String): DataSet[HVACData] = {

      env.readCsvFile[HVACData](inputPath,ignoreFirstLine = true)
        /*.map(datum => {

        println(s"next datum from HVAC: $datum")

        val fields = datum.split(",")
        HVACData(
          new DateTime(incomingFormat.parseMillis(fields(0) + " " + fields(1))), // dateTimeOfReading
          fields(2).toInt,   // actualTemp
          fields(3).toInt,   // targetTemp
          fields(4).toInt,     // systemID
          fields(5).toInt,     // systemAge
          fields(6).toInt      // buildingID
        )
      })*/

  }

  class HVACToBuildingMapper
    extends RichMapFunction  [HVACData,EnhancedHVACTempReading] {

    var allBuildingDetails: Map[Int, BuildingInformation] = _

    override def open(configuration: Configuration): Unit = {

      allBuildingDetails =
        getRuntimeContext
        .getBroadcastVariableWithInitializer(
          "buildingData",
          new BroadcastVariableInitializer [BuildingInformation,Map[Int,BuildingInformation]] {

            def initializeBroadcastVariable(valuesPushed:java.lang.Iterable[BuildingInformation]): Map[Int,BuildingInformation] = {
              valuesPushed
                .asScala
                .toList
              .map(nextBuilding => (nextBuilding.buildingID,nextBuilding))
              .toMap
            }
          }
        )
    }

    override def map(nextReading: HVACData): EnhancedHVACTempReading = {
      val buildingDetails = allBuildingDetails.getOrElse(nextReading.buildingID,UndefinedBuildingInformation)

      val difference = nextReading.targetTemp - nextReading.actualTemp

      val (rangeOfTempRecorded,isExtremeTempRecorded) =

        if (difference > 5 )        ("COLD",  1)
          else if (difference < 5)  ("HOT",   1)
                else                ("NORMAL",0)

      EnhancedHVACTempReading(
        nextReading.buildingID,
        rangeOfTempRecorded,
        isExtremeTempRecorded,
        buildingDetails.country,
        buildingDetails.productID,
        buildingDetails.buildingAge,
        buildingDetails.buildingManager
      )
    }

  }
}
