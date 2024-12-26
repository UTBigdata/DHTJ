package DistributedFramework

import DataPreprocess.GeneralFunctionSets.{dayOfYear_long, transTimeToTimestamp}
import GLTC.afc_patterns_generate1
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.collection.mutable.ArrayBuffer
//Index type： STP-Tree
object DHTJ1 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.
      builder().
      getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._
    val tau = 0.1 //Similarity threshold
    val startTime =  transTimeToTimestamp("2018-09-01 00:00:00")

    //"apidN","partIds"
    val apidN_partIds = spark.read.parquet("/data/apidN_partIds").
      map(line =>(line(0).toString.toInt, line(1).toString.split(",").map(_.toInt)))
    val afcidN_partIds = spark.read.parquet("/data/afcidN_partIds").
      map(line =>(line(0).toString.toInt, line(1).toString.split(",").map(_.toInt)))
    val apidN2partIds = sc.broadcast(apidN_partIds.collect().toMap)
    val afcidN2partIds = sc.broadcast(afcidN_partIds.collect().toMap)
    val numPartitions1 = apidN2partIds.value.values.map(v => v.max).max + 1
    val afcPartitioner = new TraPartitions(numPartitions1)
    val apPartitioner = new TraPartitions(numPartitions1)

    val seg2num = sc.textFile("/data/seg_num").map(v => {
      val tmp = v.split(",")
      (tmp(0),tmp(1).toInt)
    }).collect().toMap

    val stationFile = sc.textFile("/data/stationInfo-UTF-8.txt")
    val stationName2No = stationFile.map(line => {
      val stationNo = line.split(',')(0)
      val stationName = line.split(',')(1)
      (stationName, stationNo.toInt)
    }).collect().toMap
    val stationNo2Name = stationName2No.map(v=>(v._2,v._1))

    val readODTimeInterval = sc.textFile("/data/shortpath2.txt").map(line => {
      val p = line.split(' ')
      val sou = p(0).toInt
      val des = p(1).toInt
      val interval = math.ceil((p(2).toDouble*60)).toLong
      ((sou, des), interval)
    })
    val ODIntervalMap = sc.broadcast(readODTimeInterval.collect().toMap)

    val realValidPathFile = sc.textFile("/data/realValidPaths").map(line => {
      val path = line.split(" ").map(_.toInt)
      ((path.head,path.last),path)
    })
    val ODs = realValidPathFile.map(_._1).distinct().collect()
    // 读取所有有效路径的数据 "1 2 3 4 5 # 0 V 0.0000 12.6500"  “O ... D # 换乘次数 V 换乘时间 总时间”
    val validPathFile = sc.textFile("/data/allpath2.txt").map(line => {
      val tmp = line.split(' ')
      val fields = tmp.dropRight(5)
      val sou = fields(0).toInt
      val des = fields(fields.length - 1).toInt
      val path = fields.map(x => x.toInt)

      ((sou, des), path)
    }).filter(line => ! ODs.contains(line._1))//RDD[((sou, des), path]
    val validPathMap: Broadcast[Map[(Int, Int), Array[Array[Int]]]] = sc.broadcast( realValidPathFile.union(validPathFile).
      groupByKey().mapValues(_.toArray).collect().toMap )

    // flow distribution "蛇口港,黄贝岭,0,0,0,259,193,173,223,350,821,903,338,114"
    val flowDistribution = sc.textFile("/data/flowMap").map(line => {
      val fields = line.split(",")
      val os = stationName2No(fields(0))
      val ds = stationName2No(fields(1))
      val flow: Array[Int] = fields.takeRight(12).map(_.toInt)
      ((os, ds), flow)
    })
    val flowMap = sc.broadcast(flowDistribution.collect().toMap)

    val mostViewPathFile = sc.textFile("/data/MostViewPath").map(line => {
      val path = line.split(",").map(station => stationName2No(station))
      val so = path.head
      val sd = path.last
      ((so, sd), path)
    })
    val mostViewPathMap = sc.broadcast(mostViewPathFile.collect().toMap)

    val tmp_segN2overlapSegNs = spark.read.parquet("/data/segN2overlapSegNs").
      map(line => (line(0).toString.toInt, line(1).toString.split(",").map(_.toInt)) )
    val segN2overlapSegNs = sc.broadcast(tmp_segN2overlapSegNs.collect().toMap)

    //段，访问过该段的AFCID集（某次行程以该段为有效路径）
    //"segN","afcidN2tripIdays" 多个afcidN之间以分号隔开 afcidN:多个以 segN 为有效路径的行程日期之间逗号隔开，行程索引和日期之间“#”隔开  一个afcId可能会在一天访问同一个OD多次
    val segN_afcidTripIdays = spark.read.parquet("/ytl/gltc/segN_afcidN2tripIdays").
      map(line => (line(0).toString.toInt,line(1).toString.split(";").map(v =>
        (v.split(":")(0).toInt,v.split(":")(1).split(",").map(_.split("#")).map(x=>(x(0).toInt,x(1).toInt)))
      )))// (segN, Array[(afcidN, Array[(tripIndex,day)])])
    val segN2afcidTripIdays = sc.broadcast(segN_afcidTripIdays.collect().toMap)

    /**
      * AFC data: (669404508,2019-06-01 09:21:28,世界之窗,21,2019-06-01 09:31:35,深大,22)
      */
    val AFCFile = sc.textFile("/data/AFCtrip").map(line => {
      val fields = line.split(',')
      val id = fields(0).toInt
      val ot = transTimeToTimestamp(fields(1))
      val os = stationName2No(fields(2))
      val dt = transTimeToTimestamp(fields(4))
      val ds = stationName2No(fields(5))
      val o_day = dayOfYear_long(ot) //2019-06-01 的 dayOfMonth 应该是 1
      val d_day = dayOfYear_long(dt)
      val day = if (o_day == d_day) o_day else 0
      (id, (ot, os, dt, ds, day))
    }).filter(line=> line._2._5 > 0 && line._2._2 != line._2._4)

    val AFCTrips = AFCFile.groupByKey().map(line => {
      val trips = line._2.toList.sortBy(_._1)
      val numDays = trips.map(_._5).toSet.size
      (line._1, trips, numDays) //(id, trips, numDays)
    })

    val AFCPatterns = AFCTrips.filter(line => afcidN2partIds.value.contains(line._1)).map(line => {
      val pairs = new ArrayBuffer[(Long, Int, Long, Int, Int)]()
      for (i <- line._2.indices) {
        val trip = line._2(i)
        pairs.append((trip._1, trip._2, trip._3, trip._4, i))
      }
      val numDays = line._3
      val afc_patterns: List[List[Int]] = afc_patterns_generate1(pairs, numDays)
      val numOccasionalTrips = pairs.size - afc_patterns.map(_.size).sum
      val OOD_ub = 1.0/2 * (pairs.size + numOccasionalTrips)

      val pairs1 = pairs.map(trip =>{
        ((trip._1 - startTime).toInt, trip._2, (trip._3 - startTime).toInt, trip._4, trip._5)
      })

      (line._1, (pairs1.toArray, afc_patterns),OOD_ub/pairs.size)
    }).filter(_._3 >= tau). /** Pruning the AFC trajectories with the global upper bound less than tau*/
      flatMap(line => afcidN2partIds.value(line._1).map(partId =>(partId,(line._1,line._2)))).
      partitionBy(afcPartitioner).map(_._2)

    val APFile = sc.textFile("/data/APtrip").map(line => {
      val fields = line.replaceAll("[()]","").split(",")
      val id = fields(0).toInt
      val ot = transTimeToTimestamp(fields(1))
      val os = stationName2No(fields(2))
      val dt = transTimeToTimestamp(fields(3))
      val ds = stationName2No(fields(4))
      val o_day = dayOfYear_long(ot)
      val d_day = dayOfYear_long(dt)
      val day = if (o_day == d_day) o_day else 0
      (id, ((ot - startTime).toInt, os, (dt - startTime).toInt, ds, day))
    }).filter(line => line._2._5 > 0 && apidN2partIds.value.contains(line._1)).
      map(line => (line._1, (line._2._1,line._2._2,line._2._3,line._2._4))) //(ot, os, dt, ds)

    val APTrips = APFile.groupByKey().map(line => {
      val apId = line._1
      val trips = line._2.toArray.sortBy(_._1)
      (apId, trips)
    }).flatMap(line => apidN2partIds.value(line._1).map(partId =>(partId,line))).
      partitionBy(apPartitioner).map(_._2)
    val µW = 3.0

    //join会导致内存溢出
    //((Iterator[(Int, Array[(Int, Int, Int, Int)])], Iterator[(Int, Array[(Int, Int, Int, Int, Int)], List[List[Int]])]) => Iterator[Nothing]) => RDD[Nothing]
    APTrips.zipPartitions(AFCPatterns){ (apIter, afcIter)=>{
      val str_afcIter = afcIter.map(v => (v._1+":"+v._2._1.map(_.toString.replaceAll("[()]","")).mkString("#")+":"+v._2._2.map(_.mkString(",")).mkString("#"))).mkString(";")
      val str_apIter = apIter.map(v => v._1+":"+v._2.map(_.toString.replaceAll("[()]","")).mkString("#")).mkString(";")
      Iterator(str_apIter, str_afcIter)
    }}.mapPartitionsWithIndex{(index,iter) =>{

      val iterArr = iter.toArray
      val apIter = iterArr(0).split(";").map(apidNRec => {
        val fields = apidNRec.split(":")
        val trips = fields(1).split("#").map(strtrip => {
          val tmp1 = strtrip.split(",").map(_.toInt)
          (tmp1(0),tmp1(1),tmp1(2),tmp1(3))
        })
        (fields(0).toInt, trips)
      }).iterator
      val afcIter = iterArr(1).split(";").map(afcidNRec=>{
        val fields = afcidNRec.split(":")
        val trips = fields(1).split("#").map(strtrip => {
          val tmp1 = strtrip.split(",").map(_.toInt)
          (tmp1(0),tmp1(1),tmp1(2),tmp1(3),tmp1(4))
        })
        var patterns = List[List[Int]]()
        if(fields.size == 3)
          patterns = fields(2).split("#").map(pattern => pattern.split(",").map(_.toInt).toList).toList
        (fields(0).toInt, (trips,patterns))
      })

      val res = ArrayBuffer[(Int, Int, Double)]()//apidN, afcidN, sim
      while (apIter.hasNext) {
        val Tw: (Int, Array[(Int, Int, Int, Int)]) = apIter.next()
        val apTrips = Tw._2

        val afcidN2TripsPatterns = afcIter.map(v => {
          val afcTrips = v._2._1
          /** Length-based Filtering */
          var sim_ub = 0d
          if (afcTrips.size <= apTrips.size) {
            val afc_patterns: List[List[Int]] = v._2._2
            val numOccasionalTrips = afcTrips.size - afc_patterns.map(_.size).sum
            val OOD_ub = 1.0 / 2 * (afcTrips.size + numOccasionalTrips)
            sim_ub = OOD_ub / (afcTrips.size + (apTrips.size - afcTrips.size) * µW)
          } else {
            sim_ub = apTrips.size * 1.0 / afcTrips.size
          }
          (v._1, v._2, sim_ub) //(afcId, (afcTrips, afcPattern), sim_ub)
        }).filter(_._3 >= tau).map(v => (v._1, v._2)).toMap
        /** TP-Tree based Filtering */
        val afcidN_rawOVTripPairs = apTrips.zipWithIndex.flatMap(apTrip_index=>{
          val apTrip = apTrip_index._1
          val index_ap = apTrip_index._2
          val day = dayOfYear_long(apTrip._1 + startTime)
          val paths: Array[Array[Int]] = validPathMap.value((apTrip._2, apTrip._4))
          val afcidN2tripIs = paths.flatMap(path => {
            val seg = path.map(v => stationNo2Name(v)).mkString("-")
            val ovsegNs = segN2overlapSegNs.value(seg2num(seg)).toSet
            val afcidN2tripIs_path: Set[(Int, Array[Int])] = ovsegNs.flatMap(ovsegN =>{
              //segN2afcidTripIdays: (segN, Array[(afcidN, Array[(tripIndex,day)])])
              segN2afcidTripIdays.value.getOrElse(ovsegN,Array[(Int, Array[(Int,Int)])]()).
                map(v => (v._1,v._2.filter(_._2==day).map(_._1))).
                filter(v => v._2.size>0 && afcidN2TripsPatterns.contains(v._1))
            })//Array[(afcidN, Array(tripIndex))]
            afcidN2tripIs_path
          }).groupBy(_._1).toArray.map(v => (v._1, v._2.flatMap(_._2).distinct))
          afcidN2tripIs.map(v => (v._1,v._2.map(ov_index_afc => (index_ap,ov_index_afc)))) //Array[(afcidN, Array[(index_ap,ov_index_afc)])]
        }).groupBy(_._1).toArray.map(v => (v._1,v._2.flatMap(_._2)))
        //由于索引树搜索的结果中，存在一个afc行程与多个ap行程重叠或一个ap行程与多个afc行程重叠，此时需要进行去重，每个ap行程只保留一个重叠afc
        val afcs = afcidN_rawOVTripPairs.map(v => (v._1, getOVtripPairs(v._2,afcidN2TripsPatterns(v._1)._2.flatMap(x => x).toArray))).//第二个参数：afcidN的retularAFCTripIs
          map(v => {
          val ovAFCTripIs = v._2.filter(ov => {
            val cur_ap = apTrips(ov._1)
            val cur_afc = afcidN2TripsPatterns(v._1)._1(ov._2)
            cur_ap._1 > cur_afc._1 - 600 && cur_ap._3 < cur_afc._3 + 600 //Temporal filtering
          }).map(_._2)
          (v._1, ovAFCTripIs)
        }).filter(_._2.size > 0)

        val afcs1  = afcs.map(v => {
          val afcidN = v._1
          val ovAFCTripIs = v._2
          val afcTrips = afcidN2TripsPatterns(afcidN)._1

          val num_regularTripPairs = afcidN2TripsPatterns(afcidN)._2.map(pattern => pattern.intersect(ovAFCTripIs).size).sum
          val numOccaTripPairs = ovAFCTripIs.size - num_regularTripPairs
          val OOD_ub1 = 1.0 / 2 * num_regularTripPairs + numOccaTripPairs
          val num_restAPTrips = math.max(0, apTrips.size - ovAFCTripIs.size)
          val num_restAFCTrips = math.max(0, afcTrips.size - ovAFCTripIs.size)
          val sim_ub1 = OOD_ub1 / (ovAFCTripIs.size + num_restAPTrips * µW + num_restAFCTrips)
          (afcidN, afcidN2TripsPatterns(afcidN), sim_ub1)
        }).filter(_._3 >= tau).sortBy(-_._3).
          map(v => ((v._1, v._2._1, v._2._2), v._3)) //((afcidN, 行程集，patterns), sim_ub1)

        /** Verification */
        if(afcs1.size > 0) {
          var Tf = afcs1(0)._1
          var sim_ub1_Tf = afcs1(0)._2
          var maxSim = GLTC.sim1(Tw, Tf,
            ODIntervalMap.value,
            validPathMap.value,
            mostViewPathMap.value,
            flowMap.value,
            startTime)._3
          var flag = true
          for (i <- 1 until afcs1.size if flag) {
            if (afcs1(i)._2 < maxSim) flag = false
            else {
              val sim = GLTC.sim1(Tw, afcs1(i)._1,
                ODIntervalMap.value,
                validPathMap.value,
                mostViewPathMap.value,
                flowMap.value,
                startTime)._3
              if (sim > maxSim) {
                Tf = afcs1(i)._1
                maxSim = sim
                sim_ub1_Tf = afcs1(i)._2
              }
            }
          } //end for
          res += ((Tw._1, Tf._1, maxSim))
        } else {
          res += ((Tw._1, -1, 0))
        }
      } //end while(apIter.hasNext)

      res.iterator
    }}.toDF("apidN","afcidN","sim").write.mode("overwrite").parquet(s"/data/dhtj_STP-Tree_simpair")

  }//end main

  //索引树搜索的结果中，存在一个afc行程与多个ap行程重叠或一个ap行程与多个afc行程重叠，此时需要进行去重，每个ap行程只保留一个重叠afc
  //原则：当一个ap行程与多个afc行程重叠，过滤这些afc行程中作为前面ap行程和后续ap行程的重叠行程，然后在剩余的行程中，优先挑选偶尔行程作为当前ap行程的偶尔行程
  //例子：ArrayBuffer((0,3), (1,10), (2,19), (2,21), (3,21), (4,20), (4,22))
  //例子：ArrayBuffer((0,1), (1,1), (1,3), (2,4))
  //例子：ArrayBuffer((0,1), (1,7), (4,8), (4,10), (5,9), (5,11), (6,8), (6,10), (7,9), (7,11), (8,14), (9,15), (10,17), (11,18), (12,19))
  def getOVtripPairs(ovTripPairs: Array[(Int,Int)], retularAFCTripIs: Array[Int])={

    val res = ArrayBuffer[(Int, Int)]()//(index_ap, index_afc)
    val apTripI2ovAFCTripIs = ovTripPairs.groupBy(_._1).map(v => (v._1,v._2.map(_._2).sorted)).toArray.sortBy(_._1) //按照index_ap升序排列
    apTripI2ovAFCTripIs.foreach(v =>{
      val index_ap = v._1
      val ovAFCTripIs = v._2.diff(res.map(_._2))
      var targetTripI = (-1)
      if(ovAFCTripIs.size == 1) targetTripI = ovAFCTripIs(0)
      else if(ovAFCTripIs.size > 1) {
        val ovAFCTripI_lastAP = if(res.size > 0) res.last._2 else (-1)
        val ovAFCTripIs1 = ovAFCTripIs.filter(_ > ovAFCTripI_lastAP)
        if(ovAFCTripIs1.size == 1) targetTripI = ovAFCTripIs1(0)
        else if(ovAFCTripIs1.size >1){

          val ovAFCTripIs_subsequentAP = apTripI2ovAFCTripIs.filter(_._1> index_ap).flatMap(_._2).distinct
          val tmp = ovAFCTripIs1.diff(ovAFCTripIs_subsequentAP)
          if(tmp.size == 0) targetTripI = ovAFCTripIs1(0)
          else if(tmp.size == 1 )  targetTripI = tmp(0)
          else{//tmp.size > 1
          val occaTripIs = tmp.diff(retularAFCTripIs)
            if(occaTripIs.size>0) targetTripI = occaTripIs(0)
            else targetTripI = tmp(0)
          }
        }//end else if(ovAFCTripIs1.size >1)
      }//end else if(ovAFCTripIs.size > 1)
      if(targetTripI > (-1)) res += ((index_ap,targetTripI))
    })
    res.toArray
  }//end func

}//end object
