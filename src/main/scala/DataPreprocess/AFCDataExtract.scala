package DataPreprocess

import org.apache.spark.sql.SparkSession
import scala.collection.mutable.ArrayBuffer

object AFCDataExtract {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    val rdd = spark.read.option("header",true).csv(s"/data/AFCdata").rdd.map(fields => {
      val afcidN = fields(0).toString.toInt
      val time = fields(1).toString
      val station = fields(2).toString
      val tag = fields(3).toString.toInt //21: tap-in, 22: tap-out
      (afcidN,(time, station, tag))
    }).groupByKey().map(v =>(v._1,v._2.toArray.sortBy(_._1)))

    //Trajectory Segmentation
    val arr = new ArrayBuffer[String]()
    rdd.flatMap({ fields =>
      arr.clear()
      val tra: Array[(String, String, Int)] = fields._2

      var i = 0
      while (i < tra.length - 1) {
        if (tra(i)._3==21  && tra(i + 1)._3==22) {
          arr += Array(fields._1,tra(i)._1,tra(i)._2,tra(i)._3,tra(i + 1)._1,tra(i + 1)._2,tra(i + 1)._3).mkString(",")

          i = i + 2
        } else {
          i = i + 1
        }
      } //end while

      arr
    }).saveAsTextFile("/data/AFCtrip")

  }//end main

}
