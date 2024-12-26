package DistributedFramework

import DataPreprocess.GeneralFunctionSets.{hourOfDay_long,dayOfYear_long,secondsOfDay}

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.math._

object GLTC {
  def sim(ap: (String, Array[(Long, String, Long, String, Int)]), //ap: (apId, trips)
          afc: (String, Array[(Long, String, Long, String, Int)], List[List[Int]]),//afc: (id，出行片段集合，出行模式数组(包含出行索引信息)) Array[(Long, String, Long, String, Int)]第个元素可能是day也可能是行程下标，取决于谁调用这个函数
          ODIntervalMap: Map[(String, String), Long],
          validPathMap: Map[(String, String), Array[Array[String]]],
          mostViewPathMap: Map[(String, String), Array[String]],
          flowMap: Map[(String, String), Array[Int]]
         )={
    val gama_1 = 0.3
    val gama_2 = 0.2
    val gama_3 = 0.04
    val µN = 6.0
    val µW = 3.0
    val APId = ap._1
    val AFCId = afc._1

    val AP = ap._2 //Array[(ot, os, dt, ds, day)]  该AP的所有行程的集合
    val AFC = afc._2 //Array[(ot, os, dt, ds, day 或 行程的索引（下标从0开始）)]  AFCId的所有行程的集合
    val tr_ap_afc = new ArrayBuffer[(Int, Int)]()
    val tr_ap = new ArrayBuffer[Int]()
    val tr_afc = new ArrayBuffer[Int]()
    var index_ap = 0
    var index_afc = 0
    var conflict = new ListBuffer[(Int, Int)]

    while (index_ap < AP.length && index_afc < AFC.length) {
      val cur_ap = AP(index_ap) //(ot, os, dt, ds, day)
      val cur_afc = AFC(index_afc) //(ot, os, dt, ds,行程的索引（下标从0开始）)
      if (cur_ap._3 < cur_afc._1) { //cur_ap.dt < cur_afc.ot
        tr_ap.append(index_ap)
        index_ap += 1
      }
      else if (cur_ap._1 > cur_afc._3) { //cur_ap.ot > cur_afc.dt
        tr_afc.append(index_afc)
        index_afc += 1
      }
      else if (cur_ap._1 > cur_afc._1 - 600 && cur_ap._3 < cur_afc._3 + 600) {
        val paths = validPathMap((cur_afc._2, cur_afc._4)) //validPathMap: Map[(sou, des), Array[path]]
        var flag = true
        for (path <- paths if flag) {
          if (path.indexOf(cur_ap._2) >= 0 && path.indexOf(cur_ap._4) > path.indexOf(cur_ap._2)) { //cur_ap._2: samp_os, cur_ap._4: samp_ds
            val interval1 = ODIntervalMap(path.head, cur_ap._2)
            val headGap = cur_ap._1 - cur_afc._1
            val interval2 = ODIntervalMap(cur_ap._4, path.last)
            val endGap = cur_afc._3 - cur_ap._3
            if (headGap < 600 + interval1) {
              flag = false
              tr_ap_afc.append((index_ap, index_afc))
            }
          }
        }
        if (flag) {
          conflict.append((index_afc, index_ap))
        }
        index_afc += 1
        index_ap += 1
      }
      else {
        conflict.append((index_afc, index_ap))
        index_afc += 1
        index_ap += 1
      }
    }
    val conflictRatio = conflict.length.toDouble / (AP.length + AFC.length)

    // key:afc_index, value:(ap_index, score)
    var OL: Map[Int, (Int, Double)] = Map()
    var afc_pattern = List[List[Int]]()
    if(afc._3.size > 0) afc_pattern = afc._3
    else{
      val pairs = new ArrayBuffer[(Long, String, Long, String, Int)]()
      for (i <- AFC.indices) { //line._2 : Array[(ot, os, dt, ds, day)] 当前id的所有行程的集合
        val trip = AFC(i)
        pairs.append((trip._1, trip._2, trip._3, trip._4, i)) //(ot, os, dt, ds,行程的索引（下标从0开始）) 当前行程为该id的第i次行程，其索引为i-1
      }
      val numDays = AFC.map(_._5).distinct.size
      afc_pattern = afc_patterns_generate(pairs, numDays)
    }
    val score = new ListBuffer[Double]()
    var Similarity = 0d
    if (conflictRatio <= 0.1) {
      if (tr_ap_afc.nonEmpty) {
        for (pair <- tr_ap_afc) { //VR trip pairs
          val trip_ap = AP(pair._1) //AP：Array[(ot, os, dt, ds, day)]
          val trip_afc = AFC(pair._2) //AFC: Array[(ot, os, dt, ds,行程的索引（下标从0开始）)]
          val ol_1 = min((trip_ap._3 - trip_ap._1).toFloat / (trip_afc._3 - trip_afc._1), 1)
          val ot_ap = hourOfDay_long(trip_ap._1) / 2
          val flow_ap = flowMap((trip_ap._2, trip_ap._4))(ot_ap)
          val ot_afc = hourOfDay_long(trip_afc._1) / 2
          val flow_afc = flowMap((trip_afc._2, trip_afc._4))(ot_afc)
          val ol_2 = min(flow_afc.toFloat / flow_ap, 1)
          OL += (pair._2 -> (pair._1, (1 - gama_1) * ol_1 + gama_1 * ol_2))
        }

        var index = Set[Int]()
        for (pattern <- afc_pattern) {
          val ap_seg = new ArrayBuffer[Int]()
          val group_scores = new ArrayBuffer[Double]()
          for (i <- pattern) {
            if (OL.contains(i)) {
              index += i
              ap_seg.append(OL(i)._1)
              group_scores.append(OL(i)._2)
            }
          }

          if (ap_seg.nonEmpty) {
            val cur_afc = AFC(pattern.head) //Array[(ot, os, dt, ds,行程的索引（下标从0开始）)]  AFCId的所有行程的集合 默认选择第一个AFC行程与聚合AP行程计算agg_score
            var agg_score = 0d
            val path = mostViewPathMap((cur_afc._2, cur_afc._4))
            var mostLeft = path.length
            var mostRight = -1
            var belongSamePath = true
            for (i <- ap_seg if belongSamePath) {
              val ap_os = AP(i)._2
              val ap_ds = AP(i)._4
              val left = path.indexOf(ap_os)
              val right = path.indexOf(ap_ds)
              if (left >= 0 & right > left) {
                mostLeft = min(mostLeft, left)
                mostRight = max(mostRight, right)
              }
              else {
                belongSamePath = false
              }
            }

            var time_ratio = 0f
            var flow_ratio = 0f
            if (belongSamePath & mostLeft < mostRight) {
              val agg_os = path(mostLeft)
              val agg_ds = path(mostRight)
              val agg_trip_time = ODIntervalMap((agg_os, agg_ds))
              time_ratio = min(agg_trip_time.toFloat / (cur_afc._3 - cur_afc._1), 1)
              val ot = hourOfDay_long(cur_afc._1) / 2
              val flow_afc = flowMap((cur_afc._2, cur_afc._4))(ot)
              val flow_ap = flowMap((agg_os, agg_ds))(ot)
              flow_ratio = min(flow_afc.toFloat / flow_ap, 1)
            }else {
              val agg_trip = AP(ap_seg.maxBy(x => AP(x)._3 - AP(x)._1))
              time_ratio = min((agg_trip._3 - agg_trip._1).toFloat / (cur_afc._3 - cur_afc._1), 1)
              val ot = hourOfDay_long(cur_afc._1) / 2
              val flow_afc = flowMap((cur_afc._2, cur_afc._4))(ot)
              val flow_ap = flowMap((agg_trip._2, agg_trip._4))(ot)
              flow_ratio = min(flow_afc.toFloat / flow_ap, 1)
            }
            agg_score = (1 - gama_1) * time_ratio + gama_1 * flow_ratio

            // 衰减
            var group_score = 0d
            val sort_a = group_scores.sorted
            for (i  <- group_scores.indices) {  //1/(1+e^(0.04*x))
              group_score += (gama_2 * agg_score + (1 - gama_2) * sort_a(i) )/ (1 + Math.exp(gama_3 * i))//原分母：Math.exp(gama_3 * i) 权重设置与论文描述的完全相反
            }
            score.append(group_score)
          }
        }//for (pattern <- afc_pattern)
        // 无pattern  偶尔行程对组
        score.append(OL.filter(x => !index.contains(x._1)).map(_._2._2).sum)
      }

      val tr_ap_size = AP.length - (tr_ap_afc.length + conflict.length)
      val tr_afc_size = AFC.length - (tr_ap_afc.length + conflict.length)
      Similarity = score.sum / (tr_ap_afc.length + conflict.length * µN + tr_ap_size * µW + tr_afc_size)
    }
    Tuple3(AFCId,APId,Similarity)
  }//end func

  def sim1(ap: (Int, Array[(Int, Int, Int, Int)]), //ap: (apId, trips) trip:
          afc: (Int, Array[(Int, Int, Int, Int, Int)], List[List[Int]]),//afc: (id，出行片段集合，出行模式数组(包含出行索引信息)) Array[(Long, String, Long, String, Int)]第个元素可能是day也可能是行程下标，取决于谁调用这个函数
          ODIntervalMap: Map[(Int, Int), Long],
          validPathMap: Map[(Int, Int), Array[Array[Int]]],
          mostViewPathMap: Map[(Int, Int), Array[Int]],
          flowMap: Map[(Int, Int), Array[Int]],
          startTimestamp: Long
         )={
    val gama_1 = 0.3
    val gama_2 = 0.2
    val gama_3 = 0.04
    val µN = 6.0
    val µW = 3.0
    val APId = ap._1
    val AFCId = afc._1

    val AP = ap._2.map(v => (v._1 + startTimestamp,v._2,v._3 + startTimestamp,v._4)) //Array[(ot, os, dt, ds)]  该AP的所有行程的集合
    val AFC = afc._2.map(v => (v._1 + startTimestamp,v._2,v._3 + startTimestamp,v._4,dayOfYear_long(v._1 + startTimestamp))) //Array[(ot, os, dt, ds, day 或 行程的索引（下标从0开始）)]  AFCId的所有行程的集合
    val tr_ap_afc = new ArrayBuffer[(Int, Int)]()
    val tr_ap = new ArrayBuffer[Int]()
    val tr_afc = new ArrayBuffer[Int]()
    var index_ap = 0
    var index_afc = 0
    var conflict = new ListBuffer[(Int, Int)]

    while (index_ap < AP.length && index_afc < AFC.length) {
      val cur_ap = AP(index_ap) //(ot, os, dt, ds, day)
      val cur_afc = AFC(index_afc) //(ot, os, dt, ds,行程的索引（下标从0开始）)
      if (cur_ap._3 < cur_afc._1) { //cur_ap.dt < cur_afc.ot
        tr_ap.append(index_ap)
        index_ap += 1
      }
      else if (cur_ap._1 > cur_afc._3) { //cur_ap.ot > cur_afc.dt
        tr_afc.append(index_afc)
        index_afc += 1
      }
      else if (cur_ap._1 > cur_afc._1 - 600 && cur_ap._3 < cur_afc._3 + 600) {
        val paths = validPathMap((cur_afc._2, cur_afc._4)) //validPathMap: Map[(sou, des), Array[path]]
        var flag = true
        for (path <- paths if flag) {
          if (path.indexOf(cur_ap._2) >= 0 && path.indexOf(cur_ap._4) > path.indexOf(cur_ap._2)) {
            val interval1 = ODIntervalMap(path.head, cur_ap._2)
            val headGap = cur_ap._1 - cur_afc._1
            val interval2 = ODIntervalMap(cur_ap._4, path.last)
            val endGap = cur_afc._3 - cur_ap._3
            if (headGap < 600 + interval1) {
              flag = false
              tr_ap_afc.append((index_ap, index_afc))
            }
          }
        }
        if (flag) {
          conflict.append((index_afc, index_ap))
        }
        index_afc += 1
        index_ap += 1
      }
      else {
        conflict.append((index_afc, index_ap))
        index_afc += 1
        index_ap += 1
      }
    }
    val conflictRatio = conflict.length.toDouble / (AP.length + AFC.length)

    // key:afc_index, value:(ap_index, score)
    var OL: Map[Int, (Int, Double)] = Map()
    var afc_pattern = List[List[Int]]()
    if(afc._3.size > 0) afc_pattern = afc._3
    else{
      val pairs = new ArrayBuffer[(Long, Int, Long, Int, Int)]()
      for (i <- AFC.indices) {
        val trip = AFC(i)
        pairs.append((trip._1, trip._2, trip._3, trip._4, i))
      }
      val numDays = AFC.map(_._5).distinct.size
      afc_pattern = afc_patterns_generate1(pairs, numDays)
    }
    val score = new ListBuffer[Double]()
    var Similarity = 0d
    if (conflictRatio <= 0.1) {
      if (tr_ap_afc.nonEmpty) {
        for (pair <- tr_ap_afc) { //VR trip pairs
          val trip_ap = AP(pair._1) //AP：Array[(ot, os, dt, ds, day)]
          val trip_afc = AFC(pair._2) //AFC: Array[(ot, os, dt, ds,行程的索引（下标从0开始）)]  AFCId的所有行程的集合
          val ol_1 = min((trip_ap._3 - trip_ap._1).toFloat / (trip_afc._3 - trip_afc._1), 1)

          val ot_ap = hourOfDay_long(trip_ap._1) / 2
          val flow_ap = flowMap((trip_ap._2, trip_ap._4))(ot_ap)
          val ot_afc = hourOfDay_long(trip_afc._1) / 2
          val flow_afc = flowMap((trip_afc._2, trip_afc._4))(ot_afc)
          val ol_2 = min(flow_afc.toFloat / flow_ap, 1)
          OL += (pair._2 -> (pair._1, (1 - gama_1) * ol_1 + gama_1 * ol_2))
        }

        var index = Set[Int]()
        for (pattern <- afc_pattern) {
          val ap_seg = new ArrayBuffer[Int]()
          val group_scores = new ArrayBuffer[Double]()
          for (i <- pattern) {
            if (OL.contains(i)) {
              index += i
              ap_seg.append(OL(i)._1)
              group_scores.append(OL(i)._2)
            }
          }
          // 计算每个group的得分  其实就是计算当前group的得分（每个pattern对应一个行程对组）
          if (ap_seg.nonEmpty) {
            val cur_afc = AFC(pattern.head) //Array[(ot, os, dt, ds,行程的索引（下标从0开始）)]  AFCId的所有行程的集合 默认选择第一个AFC行程与聚合AP行程计算agg_score
            var agg_score = 0d
            val path = mostViewPathMap((cur_afc._2, cur_afc._4))

            var mostLeft = path.length
            var mostRight = -1
            var belongSamePath = true
            for (i <- ap_seg if belongSamePath) {
              val ap_os = AP(i)._2
              val ap_ds = AP(i)._4
              val left = path.indexOf(ap_os)
              val right = path.indexOf(ap_ds)
              if (left >= 0 & right > left) {
                mostLeft = min(mostLeft, left)
                mostRight = max(mostRight, right)
              }
              else {
                belongSamePath = false
              }
            }

            var time_ratio = 0f
            var flow_ratio = 0f
            if (belongSamePath & mostLeft < mostRight) {
              val agg_os = path(mostLeft)
              val agg_ds = path(mostRight)
              val agg_trip_time = ODIntervalMap((agg_os, agg_ds))
              time_ratio = min(agg_trip_time.toFloat / (cur_afc._3 - cur_afc._1), 1)
              val ot = hourOfDay_long(cur_afc._1) / 2
              val flow_afc = flowMap((cur_afc._2, cur_afc._4))(ot)
              val flow_ap = flowMap((agg_os, agg_ds))(ot)
              flow_ratio = min(flow_afc.toFloat / flow_ap, 1)
            }else {
              val agg_trip = AP(ap_seg.maxBy(x => AP(x)._3 - AP(x)._1))
              time_ratio = min((agg_trip._3 - agg_trip._1).toFloat / (cur_afc._3 - cur_afc._1), 1)
              val ot = hourOfDay_long(cur_afc._1) / 2
              val flow_afc = flowMap((cur_afc._2, cur_afc._4))(ot)
              val flow_ap = flowMap((agg_trip._2, agg_trip._4))(ot)
              flow_ratio = min(flow_afc.toFloat / flow_ap, 1)
            }
            agg_score = (1 - gama_1) * time_ratio + gama_1 * flow_ratio

            // 衰减
            var group_score = 0d
            val sort_a = group_scores.sorted
            for (i  <- group_scores.indices) {
              group_score += (gama_2 * agg_score + (1 - gama_2) * sort_a(i) )/ (1 + Math.exp(gama_3 * i))
            }
            score.append(group_score)
          }
        }//for (pattern <- afc_pattern)
        // 无pattern  偶尔行程对组
        score.append(OL.filter(x => !index.contains(x._1)).map(_._2._2).sum)
      }

      val tr_ap_size = AP.length - (tr_ap_afc.length + conflict.length)
      val tr_afc_size = AFC.length - (tr_ap_afc.length + conflict.length)
      Similarity = score.sum / (tr_ap_afc.length + conflict.length * µN + tr_ap_size * µW + tr_afc_size)
    }
    Tuple3(AFCId,APId,Similarity)
  }

  case class distAndKinds(var d: Long, var k: Int)

  // 高斯核函数
  def RBF(l: Long, x: Long, h: Int): Double = {
    1 / sqrt(2 * Pi) * exp(-pow(x - l, 2) / (2 * pow(h, 2)))
  }

  // 计算相对距离
  def compute_dist(info: Array[(Double, Long)]): Array[Long] = {
    val result = new Array[Long](info.length)
    val s = mutable.Stack[Int]()
    s.push(0)
    var i = 1
    var index = 0
    while (i < info.length) {
      if (s.nonEmpty && info(i)._1 > info(s.top)._1) {
        index = s.pop()
        result(index) = abs(info(i)._2 - info(index)._2)
      }
      else {
        s.push(i)
        i += 1
      }
    }
    while (s.nonEmpty) {
      result(s.pop()) = -1
    }
    result
  }

  // 计算z_score自动选取聚类中心
  def z_score(dens_pos: Array[(Double, Long)]): Array[(Int, Long)] = {
    val dist_r = compute_dist(dens_pos)
    val dist_l = compute_dist(dens_pos.reverse).reverse
    val dist_dens_pos = new ArrayBuffer[(Long, Double, Long)]()
    for (i <- dist_r.indices) {
      if (dist_r(i) == -1 && dist_l(i) == -1)
        dist_dens_pos.append((dens_pos.last._2 - dens_pos.head._2, dens_pos(i)._1, dens_pos(i)._2))
      else if (dist_r(i) != -1 && dist_l(i) != -1)
        dist_dens_pos.append((min(dist_r(i), dist_l(i)), dens_pos(i)._1, dens_pos(i)._2))
      else if (dist_l(i) != -1)
        dist_dens_pos.append((dist_l(i), dens_pos(i)._1, dens_pos(i)._2))
      else
        dist_dens_pos.append((dist_r(i), dens_pos(i)._1, dens_pos(i)._2))
    }
    var sum_dist = 0L
    var sum_dens = 0d
    dist_dens_pos.foreach(x => {
      sum_dist += x._1
      sum_dens += x._2
    })
    val avg_dist = sum_dist / dist_dens_pos.length
    val avg_dens = sum_dens / dist_dens_pos.length
    var total = 0d
    for (v <- dist_dens_pos) {
      total += pow(abs(v._1 - avg_dist), 2) + pow(abs(v._2 - avg_dens), 2)
    }
    val sd = sqrt(total / dist_dens_pos.length)
    val z_score = new ArrayBuffer[((Long, Double, Long), Double)]()
    var z_value = 0d
    for (v <- dist_dens_pos) {
      z_value = sqrt(pow(abs(v._1 - avg_dist), 2) + pow(abs(v._2 - avg_dens), 2)) / sd
      z_score.append((v, z_value))
    }
    val result = new ArrayBuffer[(Int, Long)]()
    // z-score大于3认为是类簇中心
    val clustersInfo = z_score.toArray.filter(_._2 >= 3)
    for (i <- clustersInfo.indices) {
      result.append((i + 1, clustersInfo(i)._1._3))
    }
    result.toArray
  }

  def afc_patterns_generate1(pairs: ArrayBuffer[(Long, Int, Long, Int, Int)], numDays: Int)={

    val stampBuffer = new ArrayBuffer[Long]()
    pairs.foreach(v => {
      stampBuffer.append(secondsOfDay(v._1)) //ot
      stampBuffer.append(secondsOfDay(v._3)) //dt
    })
    val timestamps = stampBuffer.toArray.sorted
    // 设置带宽h，单位为秒
    val h = 1800

    val density_stamp_Buffer = new ArrayBuffer[(Double, Long)]()
    for (t <- timestamps) {
      var temp = 0D
      for (v <- timestamps) {
        temp += RBF(v, t, h)
      }
      density_stamp_Buffer.append((temp / (timestamps.length * h), t))
    }
    val density_stamp = density_stamp_Buffer.toArray.sortBy(_._2)


    val cluster_center: Array[(Int, Long)] = z_score(density_stamp)

    val dc = 5400

    val clusters = new ArrayBuffer[(Int, (Long, Int, Long, Int, Int))]
    for (v <- pairs) { //
      if (cluster_center.nonEmpty) {
        val o_stamp = secondsOfDay(v._1)
        val d_stamp = secondsOfDay(v._3)
        val o_to_c = distAndKinds(Long.MaxValue, 0)
        val d_to_c = distAndKinds(Long.MaxValue, 0)
        for (c <- cluster_center) {
          if (abs(o_stamp - c._2) < dc && abs(o_stamp - c._2) < o_to_c.d) {
            o_to_c.k = c._1
            o_to_c.d = abs(o_stamp - c._2)
          }
          if (abs(d_stamp - c._2) < dc && abs(d_stamp - c._2) < d_to_c.d) {
            d_to_c.k = c._1
            d_to_c.d = abs(d_stamp - c._2)
          }
        }
        if (o_to_c.k == d_to_c.k && o_to_c.k != 0)
          clusters.append((o_to_c.k, v))
        else
          clusters.append((0, v))
      }
      else
        clusters.append((0, v))
    }

    val afc_patterns = new ListBuffer[List[Int]]()

    val grouped= clusters.groupBy(_._1).toArray.filter(x => x._1 > 0)
    if (grouped.nonEmpty) {
      grouped.foreach(g => {

        val temp_data= g._2.toArray.groupBy(x => (x._2._2, x._2._4))
        temp_data.foreach(v => {

          if (v._2.length >= 5 || v._2.length > numDays / 2) {

            val temp_patterns = new ListBuffer[Int]()
            v._2.foreach(x => temp_patterns.append(x._2._5))
            afc_patterns.append(temp_patterns.toList)
          }
        })
      })
    }

    afc_patterns.toList
  }//end func

  def afc_patterns_generate(pairs: ArrayBuffer[(Long, String, Long, String, Int)], numDays: Int)={

    val stampBuffer = new ArrayBuffer[Long]()
    pairs.foreach(v => {
      stampBuffer.append(secondsOfDay(v._1)) //ot
      stampBuffer.append(secondsOfDay(v._3)) //dt
    })
    val timestamps = stampBuffer.toArray.sorted
    // 设置带宽h，单位为秒
    val h = 1800

    val density_stamp_Buffer = new ArrayBuffer[(Double, Long)]()
    for (t <- timestamps) {
      var temp = 0D
      for (v <- timestamps) {
        temp += RBF(v, t, h)
      }
      density_stamp_Buffer.append((temp / (timestamps.length * h), t))
    }
    val density_stamp = density_stamp_Buffer.toArray.sortBy(_._2)

    val cluster_center: Array[(Int, Long)] = z_score(density_stamp)

    val dc = 5400

    val clusters = new ArrayBuffer[(Int, (Long, String, Long, String, Int))]
    for (v <- pairs) { //
      if (cluster_center.nonEmpty) {
        val o_stamp = secondsOfDay(v._1)
        val d_stamp = secondsOfDay(v._3)
        val o_to_c = distAndKinds(Long.MaxValue, 0)
        val d_to_c = distAndKinds(Long.MaxValue, 0)
        for (c <- cluster_center) {
          if (abs(o_stamp - c._2) < dc && abs(o_stamp - c._2) < o_to_c.d) {
            o_to_c.k = c._1
            o_to_c.d = abs(o_stamp - c._2)
          }
          if (abs(d_stamp - c._2) < dc && abs(d_stamp - c._2) < d_to_c.d) {
            d_to_c.k = c._1
            d_to_c.d = abs(d_stamp - c._2)
          }
        }
        if (o_to_c.k == d_to_c.k && o_to_c.k != 0)
          clusters.append((o_to_c.k, v))
        else
          clusters.append((0, v))
      }
      else
        clusters.append((0, v))
    }

    val afc_patterns = new ListBuffer[List[Int]]()

    val grouped= clusters.groupBy(_._1).toArray.filter(x => x._1 > 0)
    if (grouped.nonEmpty) {
      grouped.foreach(g => {

        val temp_data= g._2.toArray.groupBy(x => (x._2._2, x._2._4))
        temp_data.foreach(v => {
          if (v._2.length >= 5 || v._2.length > numDays / 2) {

            val temp_patterns = new ListBuffer[Int]()
            v._2.foreach(x => temp_patterns.append(x._2._5))
            afc_patterns.append(temp_patterns.toList)
          }
        })
      })
    }

    afc_patterns.toList
  }//end func

}//end object
