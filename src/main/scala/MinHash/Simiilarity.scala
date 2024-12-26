package MinHash

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import DistributedFramework.GLTC

class Simiilarity(hashTables: RDD[(Iterable[String], Iterable[String])],
                  ODIntervalMap: Map[(Int, Int), Long],
                  validPathMap: Map[(Int, Int), Array[Array[Int]]],
                  mostViewPathMap: Map[(Int, Int), Array[Int]],
                  flowMap: Map[(Int, Int), Array[Int]],
                  tau: Double,
                  startTimestamp: Long) extends Serializable {

  def compute(outDir: String): Unit = {
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    @transient
    val result = hashTables.flatMap { row =>
      val afcRecords = row._2
      val apRecords = row._1

      afcRecords.flatMap { afcRecord =>
        val afcId = afcRecord.split(":")(1).toInt
        val afcTrips = afcRecord.split(":")(0).split(";").map(trip =>{
          val tmp = trip.split(",")
          (tmp(0).toInt, tmp(1).toInt,tmp(2).toInt, tmp(3).toInt,tmp(4).toInt)
        })
        apRecords.map{ apRecord =>
          val apId = apRecord.split(":")(1).toInt
          val apTrips = apRecord.split(":")(0).split(";").map(trip =>{
            val tmp = trip.split(",")
            (tmp(0).toInt, tmp(1).toInt,tmp(2).toInt, tmp(3).toInt)
          })
          GLTC.sim1((apId,apTrips),
            (afcId, afcTrips,List[List[Int]]()),
            ODIntervalMap,
            validPathMap,
            mostViewPathMap,
            flowMap,startTimestamp)//AFCId,APId,Similarity
        }
      }
    }.filter( x => x._3 >= tau).toDF("afcidN","apidN", "Similarity")

    result.write.mode("overwrite").parquet(outDir)

  }

}