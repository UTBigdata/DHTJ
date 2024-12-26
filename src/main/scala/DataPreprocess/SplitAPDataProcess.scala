package DataPreprocess

import org.apache.spark.sql.SparkSession

/**
  * Input: "/data/mac_trip-ap"  （"mac","trip"） (parquet格式)
  * Output: "/data/APtrip"   (apId, ot, os, dt, ds)
  * */
object SplitAPDataProcess {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().getOrCreate()
    val sc = spark.sparkContext

    //"mac","trip" 行程中轨迹点之间用分号隔开，一个轨迹点中时间和站点用逗号隔开
    val df=spark.read.parquet(s"/data/mac_trip-ap")
    df.rdd.map(fields => {
      val apId = fields(0).toString
      val trip = fields(1).toString.split(";").map(p => (p.split(",")(0), p.split(",")(1)))
      val ot = trip(0)._1
      val os = trip(0)._2
      val dt = trip(trip.size-1)._1
      val ds = trip(trip.size-1)._2
      (apId, ot, os, dt, ds) //无论是(apId, ot, os, dt, ds)还是Array(apId, ot, os, dt, ds).mkString(",")，最后的结果都有()，额很无语，spark-shell试了明明就没有
    }).saveAsTextFile(s"/data/APtrip")

  }//end main
}
