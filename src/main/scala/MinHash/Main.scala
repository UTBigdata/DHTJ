package MinHash

import java.text.SimpleDateFormat

import org.apache.spark.sql.{Dataset, SparkSession}
import DataPreprocess.GeneralFunctionSets.{dayOfYear_long, transTimeToTimestamp}
import org.apache.spark.broadcast.Broadcast

object Main {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    val afcInputdir = "/data/afcTimeSeries"
    val apInputdir = "/data/apTimeSeries"

    var outdir = "/data/lsh_simpair"
    val startTime = "2018-09-01 00:00:00"
    val endTime = "2018-09-08 23:59:59"
    val tau = 0.1
    val numHashTable = 100  //The number of bands
    val numHashFunc = 2   //The number of rows in each band
    val numPartitions = 4
    val lb = 0.95
    val ub = 1.0

    val stationFile = sc.textFile("/data/stationInfo-UTF-8.txt")
    val stationName2No = stationFile.map(line => {
      val stationNo = line.split(',')(0)
      val stationName = line.split(',')(1)
      (stationName, stationNo.toInt)
    }).collect().toMap

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

    val afcRDD = spark.read.parquet(afcInputdir).rdd.map(line =>
      (line(0).toString, line(1).toString, line(2).toString)) //"afcId","timeSeries","trips"
    val apRDD = spark.read.parquet(apInputdir).rdd.map(line =>
      (line(0).toString, line(1).toString, line(2).toString)) //"apId","timeSeries","trips"

    val locationCount = 69
    val tlCount = countTl(transTimeToTimestamp(startTime), transTimeToTimestamp(endTime),locationCount).toInt

    val lsh = new LSH(afcRDD, apRDD, countPrime(tlCount), numHashFunc, numHashTable, numPartitions, ub, lb)
    val model = lsh.run()

    val simiilarity = new Simiilarity(model.hashTables,
      ODIntervalMap.value,
      validPathMap.value,
      mostViewPathMap.value,
      flowMap.value,
      tau,
      transTimeToTimestamp(startTime))
    simiilarity.compute(outdir)

  }//end main

  def countTl(startTime: Long, endTime: Long, locationCount:Int): Long = {
    val count = dayOfYear_long(endTime) - dayOfYear_long(startTime)
    return count * locationCount
  }

  def countPrime(num: Int): Int = {
    if(isPrime(num)) return num

    var answer = num + 1
    while (!isPrime(answer))
      answer += 1

   return answer
  }

  def isPrime(num: Int): Boolean = {
    val check = true
    var i = 2
    while (i <= Math.sqrt(num)) {
      if (num % i.toInt == 0)
        return false
      i=i+1
    }
    check
  }
}

