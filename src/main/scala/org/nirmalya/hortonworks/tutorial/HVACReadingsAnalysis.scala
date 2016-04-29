package org.nirmalya.hortonworks.tutorial

import org.apache.flink.api.common.functions.{BroadcastVariableInitializer, RichMapFunction}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._
import org.apache.flink.api.java.io.CsvReader
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.joda.time.DateTime
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}
import scala.collection.breakOut

import scala.io.Source

import scala.collection.JavaConverters._


/**
 * Created by nirmalya on 27/2/16.
 */

case class HVACData(
     DateTimeOfReading: DateTime,actualTemp: Int,targetTemp: Int,
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

case class EnhancedHVACTempReading(buildingID: Int, rangeOfTemp: String, extremeIndicator: Boolean,country: String, productID: String,buildingAge: Int, buildingManager: String)

object HVACReadingsAnalysis {

  val incomingFormat = DateTimeFormat.forPattern("MM/dd/yy HH:mm:ss")

  def main(args: Array[String]): Unit = {

    val envDefault = ExecutionEnvironment.getExecutionEnvironment

    val buildingsBroadcastSet =
      envDefault
        .fromElements(readBuildingInfo(envDefault,"./SensorFiles/building.csv"))

    val hvacStream = readHVACReadings(envDefault,"./SensorFiles/HVAC.csv")

    hvacStream
      .map(new HVACToBuildingMapper)
      .withBroadcastSet(buildingsBroadcastSet,"buildingData")
      .writeAsCsv("./hvacTemp.csv")

    envDefault.execute("HVAC Simulation")

  }

  private def readBuildingInfo(env: ExecutionEnvironment, inputPath: String) = {

    Source.fromFile(inputPath).getLines.drop(1).map(datum => {

      val fields = datum.split(",")
      val building =
        BuildingInformation(
          fields(0).toInt,     // buildingID
          fields(1),           // buildingManager
          fields(2).toInt,     // buildingAge
          fields(3),           // productID
          fields(4)            // Country
        )
    }).toList
  }

  private def readHVACReadings(env: ExecutionEnvironment, inputPath: String) = {

      env.readTextFile(inputPath).map(datum => {

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
      })

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

//      allBuildingDetails =
//        extractedFromBroadcastVariable.asInstanceOf[List[BuildingInformation]]
//          .foldLeft(Map[Int,BuildingInformation]())((accu,elem) => accu + (elem.buildingID -> elem))
    }
    override def map(nextReading: HVACData): EnhancedHVACTempReading = {
      val buildingDetails = allBuildingDetails.getOrElse(nextReading.buildingID,UndefinedBuildingInformation)

      val difference = nextReading.targetTemp - nextReading.actualTemp

      val (rangeOfTempRecorded,isExtremeTempRecorded) =

        if (difference > 5 )        ("COLD",true)
          else if (difference < 5)  ("HOT",true)
                else                ("NORMAL",false)

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
