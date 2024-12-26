package TraPartitioner

import DataPreprocess.GeneralFunctionSets.{dayOfYear_long, transTimeToTimestamp}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

//索引树构建
object SegAFCIDset {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("SegAFCIDset")
      .getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._
    val indexType = "MTP-Tree" //MTP-Tree or STP-Tree

    val seg_num = sc.textFile(s"/data/seg_num").map(v => {
      val tmp = v.split(",")
      (tmp(0),tmp(1).toInt)
    })
    val seg2num = sc.broadcast(seg_num.collect().toMap)

    // 读取地铁站点名和编号映射关系 "1,机场东,22.647011,113.8226476,1268036000,268"
    val stationFile = sc.textFile("/data/stationInfo-UTF-8.txt")
    val stationNo2NameRDD = stationFile.map(line => {
      val stationNo = line.split(',')(0)
      val stationName = line.split(',')(1)
      (stationNo.toInt, stationName)
    })
    val stationNo2Name = stationNo2NameRDD.collect().toMap

    //基于真实ap轨迹数据的有效路径
    val realValidPathFile = sc.textFile("/data/realValidPaths").map(line => {
      val path = line.split(" ").map(s => stationNo2Name(s.toInt))
      ((path.head,path.last),path.mkString("-"))
    })
    val ODs = realValidPathFile.map(_._1).distinct().collect()
    // 读取所有有效路径的数据 "1 2 3 4 5 # 0 V 0.0000 12.6500"  “O ... D # 换乘次数 V 换乘时间 总时间”
    val validPathFile = sc.textFile("/data/allpath2.txt").map(line => {
      val tmp = line.split(' ')
      val fields = tmp.dropRight(5) //只保留“1 2 3 4 5”
      val sou = stationNo2Name(fields(0).toInt)
      val des = stationNo2Name(fields(fields.length - 1).toInt)
      val path = fields.map(x => stationNo2Name(x.toInt)).mkString("-")

      ((sou, des), path)
    }).filter(line => ! ODs.contains(line._1))//RDD[((sou, des), path]
    val validPathMap: Broadcast[Map[(String, String), Array[String]]] = sc.broadcast(realValidPathFile.union(validPathFile).
      groupByKey().mapValues(_.toArray).collect().toMap )

    //AFC data: (669404508,2019-06-01 09:21:28,世界之窗,21,2019-06-01 09:31:35,深大,22)
    val AFCTrips: RDD[(Int, List[(Long, String, Long, String, Int)], Int)] = sc.textFile("/data/AFCtrip").map(line => {
      val fields = line.split(',')
      val id = fields(0).toInt
      val ot = transTimeToTimestamp(fields(1))
      val os = fields(2)
      val dt = transTimeToTimestamp(fields(4))
      val ds = fields(5)
      val o_day = dayOfYear_long(ot)
      val d_day = dayOfYear_long(dt)
      val day = if (o_day == d_day) o_day else 0
      (id, (ot, os, dt, ds, day))
    }).filter(line => line._2._5 > 0 && line._2._2 != line._2._4).
      groupByKey().map(line => {
      val trips = line._2.toList.sortBy(_._1)
      val numDays = trips.map(_._5).toSet.size
      (line._1, trips, numDays) //(id, trips, numDays)
    }).persist(StorageLevel.MEMORY_ONLY)

    /**这部分的输出用于分区*/
    val segN_afcidNset = AFCTrips.flatMap(line => {
      val afcidN = line._1
      val trips = line._2
      trips.flatMap(trip =>{
        val paths = validPathMap.value((trip._2,trip._4))
        paths.map(path => (seg2num.value(path), afcidN))
      })
    }).groupByKey().map(v => (v._1, v._2.toArray.distinct))
    segN_afcidNset.map(v=>(v._1,v._2.mkString(","))).toDF("segN","afcidNset").
      write.mode("overwrite").parquet("/data/segN2afcidNset")

    /** 这部分的输出用于 DIPI 索引树剪枝 */
    val start = System.currentTimeMillis()/1000
    if(indexType.contains("MTP-Tree")){
      AFCTrips.flatMap(line=>{
        val pairs = new ArrayBuffer[(Long, String, Long, String, Int, Int)]() //(ot, os, dt, ds, day,行程的索引（下标从0开始）)
        for (i <- line._2.indices) { //line._2 : Array[(ot, os, dt, ds, day)] 当前id的所有行程的集合
          val trip = line._2(i)
          pairs.append((trip._1, trip._2, trip._3, trip._4, trip._5, i)) //(ot, os, dt, ds, day, 行程的索引（下标从0开始）) 当前行程为该id的第i次行程，其索引为i-1
        }
        pairs.flatMap(trip=>{
          val paths = validPathMap.value((trip._2,trip._4)).map(path => seg2num.value(path))
          paths.map(path => ((path,trip._5),(line._1,trip._6))) //((pathN,day) (afcidN, 以path为有效路径的行程索引/下标))
        })
      }).groupByKey().map(line => {
        val afcidN2tripIs = line._2.groupBy(_._1).map(v => (v._1+":"+v._2.toArray.map(_._2).mkString(",")))//afcidN:多个以path为有效路径的行程索引之间逗号隔开
        (line._1._1, line._1._2,afcidN2tripIs.mkString(";")) //多个afcidN之间以分号隔开
      }).toDF("segN","day","afcidN2tripIs").write.mode("overwrite").parquet("/data/segNday2afcidNsetWithTripIndex")
    }else if(indexType.contains("STP-Tree")){
      AFCTrips.flatMap(line=>{
        val pairs = new ArrayBuffer[(Long, String, Long, String, Int, Int)]() //(ot, os, dt, ds, day,行程的索引（下标从0开始）)
        for (i <- line._2.indices) { //line._2 : Array[(ot, os, dt, ds, day)] 当前id的所有行程的集合
          val trip = line._2(i)
          pairs.append((trip._1, trip._2, trip._3, trip._4, trip._5, i)) //(ot, os, dt, ds, day, 行程的索引（下标从0开始）) 当前行程为该id的第i次行程，其索引为i-1
        }
        pairs.flatMap(trip=>{
          val paths = validPathMap.value((trip._2,trip._4)).map(path => seg2num.value(path))
          paths.map(path => (path,(line._1,trip._6,trip._5))) //(pathN，(afcidN, 以path为有效路径的行程索引/下标,day: afcidN访问这个行程的日期))
        })
      }).groupByKey().map(line => {
        val afcidN2tripIdays = line._2.groupBy(_._1).map(v => (v._1+":"+v._2.toArray.map(x =>x._2+"#"+x._3).mkString(",")))//afcidN:多个以path为有效路径的行程日期之间逗号隔开（日期和行程索引之间“#”隔开）
        (line._1,afcidN2tripIdays.mkString(";")) //多个afcidN之间以分号隔开
      }).toDF("segN","afcidN2tripIdays").write.mode("overwrite").parquet("/data/segN_afcidN2tripIdays")
    }else{
      throw new Exception("Invalid index type input")
    }

  }//end main
}
