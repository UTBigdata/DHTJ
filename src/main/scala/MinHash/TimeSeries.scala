package MinHash

import DataPreprocess.GeneralFunctionSets.{dayOfYear_long, transTimeToTimestamp}
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer

object TimeSeries {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._
    val startTime =  transTimeToTimestamp("2018-09-01 00:00:00") //用于控制数据集大小
    val endTime =  transTimeToTimestamp("2018-09-08 23:59:59")

    val stationFile = sc.textFile("/data/stationInfo-UTF-8.txt")
    val stationName_No = stationFile.map(line => {
      val stationNo = line.split(',')(0)
      val stationName = line.split(',')(1)
      (stationName, stationNo.toInt)
    })
    val stationName2No = sc.broadcast(stationName_No.collect().toMap)

    val stationN2index: Map[Int, Int] = stationName2No.value.values.zipWithIndex.toMap

    val AFCFile = sc.textFile("/data/AFCtrip").map(line => {
      val fields = line.split(',')
      val id = fields(0).toInt
      val ot = transTimeToTimestamp(fields(1))
      val os = stationName2No.value(fields(2))
      val dt = transTimeToTimestamp(fields(4))
      val ds = stationName2No.value(fields(5))
      val o_day = dayOfYear_long(ot)
      val d_day = dayOfYear_long(dt)
      val day = if (o_day == d_day) o_day else 0
      (id, ((ot - startTime).toInt, os, (dt - startTime).toInt,ds, day))
    }).filter(line => line._2._5 > 0 && line._2._2 != line._2._4)

    AFCFile.groupByKey().map(line => {
      val trips = line._2.toArray.sortBy(_._1)
      val numDays = trips.map(_._5).toSet.size
      (line._1, trips, numDays)
    }).map(line=>{
      val strTrips = line._2.map(v => v.toString().replaceAll("[()]","")).mkString(";")
      val timeSeries = getTimeSeries(line._2,stationN2index,dayOfYear_long(startTime))
      (line._1,timeSeries,strTrips)
    }).toDF("afcidN","timeSeries","trips").write.mode("overwrite").parquet("/data/afcTimeSeries")

    val APFile = sc.textFile("/data/APtrip").map(line => {
      val fields = line.replaceAll("[()]","").split(",")
      val id = fields(0).toInt
      val ot = transTimeToTimestamp(fields(1))
      val os = stationName2No.value(fields(2))
      val dt = transTimeToTimestamp(fields(3))
      val ds = stationName2No.value(fields(4))
      val o_day = dayOfYear_long(ot)
      val d_day = dayOfYear_long(dt)
      val day = if (o_day == d_day) o_day else 0
      (id, ((ot - startTime).toInt, os, (dt - startTime).toInt, ds, day))
    }).filter(line => line._2._5 > 0)

    APFile.groupByKey().map(line => {
      val apId = line._1
      val trips: Array[(Int, Int, Int, Int, Int)] = line._2.toArray.sortBy(_._1)
      val strTrips = trips.map(v => v.toString().replaceAll("[()]","")).mkString(";")//行程与行程之间分号隔开，行程内部逗号隔开
      val timeSeries = getTimeSeries(trips,stationN2index,dayOfYear_long(startTime))
      (apId,timeSeries,strTrips)
    }).toDF("apidN","timeSeries","trips").write.mode("overwrite").parquet("/data/apTimeSeries")

  }//end main

  def getTimeSeries(trips: Array[(Int, Int, Int, Int, Int)],stationN2index: Map[Int, Int], startDay: Int)={
    val res = ArrayBuffer[Int]()
    trips.foreach(trip=>{

      res += (trip._5 - startDay) * stationN2index.size + stationN2index(trip._2)
      res += (trip._5 - startDay) * stationN2index.size + stationN2index(trip._4)
    })
    res.distinct.mkString("->")
  }

}//end object
