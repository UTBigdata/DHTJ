package MinHash

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.Random
//szt: "afcId","timeSeries","trips"
//mac: "apId","timeSeries","trips"
class LSH(szt:RDD[(String, String, String)], mac:RDD[(String, String, String)], timelocCount:Int, numHashFunc:Int, numHashTables:Int, numPartitions:Int, ub:Double, lb:Double) extends Serializable {

  def run(): LSHModel ={

    val model = new LSHModel(timelocCount, numHashFunc, numHashTables)

    //构建最小hash签名矩阵
    model.sztHashTables = szt.flatMap(v => model.hashFunctions
      .flatMap{h => List(((v._3 + ":" + v._1, h._2 % numHashTables),h._1.minhash(v._2)))})
      .groupByKey()
      .map(x => ((x._1._2 + ":" +x._2.mkString(",")),x._1._1))
      .groupByKey()

    model.macHashTables = mac.flatMap(v => model.hashFunctions
      .flatMap{h => List(((v._3 + ":" + v._1, h._2 % numHashTables),h._1.minhash(v._2)))})
      .groupByKey()
      .map(x => ((x._1._2 + ":" + x._2.mkString(",")),x._1._1))
      .groupByKey()


    val partitioner = new TraPartitions(numPartitions)

    val sig_numSZT = model.sztHashTables.map(line => (line._1, line._2.size.toLong))
    val sig_numMAC = model.macHashTables.map(line => (line._1, line._2.size.toLong))
    val sig2numSZTnumMAC = sig_numSZT.join(sig_numMAC).collect().toMap
    val ECP = sig2numSZTnumMAC.map(v => getWgt(v._2)).sum * 1.0 / numPartitions
    //Array[(pid, (sigs, ratio, totalWeight))]
    val pid2sigsRatio = partition1(ECP, ub, lb, sig2numSZTnumMAC)

    val sig2pidRatios = pid2sigsRatio.flatMap(v=>{
      val pid = v._1
      //Array[(sig, (pid, ratio))]
      v._2._1.map(sig_info => {
        if(sig_info.contains("#"))
          (sig_info.split("#")(0),(pid,sig_info.split("#")(1).toDouble))
        else
          (sig_info,(pid,1.0))
      })
    }).groupBy(_._1).map(v => (v._1, v._2.map(_._2)))


    val sztHashTables1 = model.sztHashTables.filter(line => sig2pidRatios.contains(line._1)).flatMap(line => {
      val sig = line._1
      val szts = line._2.toArray

      val sigpid2szts = ArrayBuffer[(String, Iterable[String])]()
      if(sig2pidRatios(sig).size > 1){
        val pid2ratio = sig2pidRatios(sig)
        val numMACs = sig2numSZTnumMAC(sig)._2
        var sIndex = 0
        var eIndex = 0
        for(i <- 0 until pid2ratio.size){
          val numSZTs = ((pid2ratio(i)._2 * ECP)/numMACs).toInt
          sIndex = eIndex
          eIndex += numSZTs
          if(i < pid2ratio.size - 1)
            sigpid2szts += ((sig +"#"+ pid2ratio(i)._1, szts.slice(sIndex, eIndex).toIterable))
          else
            sigpid2szts += ((sig +"#"+ pid2ratio(i)._1, szts.slice(sIndex, szts.size).toIterable))
        }
      }else{
        sigpid2szts += ((sig +"#"+ sig2pidRatios(sig)(0)._1, szts.toIterable))
      }//end else

      sigpid2szts
    }).partitionBy(partitioner).persist(StorageLevel.MEMORY_AND_DISK)

    val macHashTables1 = model.macHashTables.filter(line => sig2pidRatios.contains(line._1)).flatMap(line =>{
      val sig = line._1
      val macs = line._2
      val pids = sig2pidRatios(sig).map(_._1)
      pids.map(pid => (sig+"#"+pid, macs))
    }).partitionBy(partitioner).persist(StorageLevel.MEMORY_AND_DISK)

    //hashTables : RDD[(Iterable[String], Iterable[String])]
    model.hashTables = macHashTables1.zipPartitions(sztHashTables1){ (macIter, sztIter) =>

      val mac = macIter.toMap
      val szt = sztIter.toMap

      // 根据 key 进行 join 操作
      val keys = mac.keySet.intersect(szt.keySet)
      keys.iterator.map(key => (mac(key), szt(key)))
    }

    return model
  }

  def getWgt(numAPnumAFC: (Long, Long)): Long ={
    numAPnumAFC._1 * numAPnumAFC._2
  }

  //一边划分分区，一边切割大分区，一边切割下一个签名以补充小分区
  def partition1(ECP: Double, ub: Double, lb: Double,
                 sig2numSZTnumMAC: Map[String, (Long, Long)])={
    val gId2sigsRatio = mutable.Map[Int,(ArrayBuffer[String], Double, Long)]()

    var signatures = sig2numSZTnumMAC.keys.toArray
    var i = 0
    var totalWeight = getWgt(sig2numSZTnumMAC(signatures(0)))
    gId2sigsRatio += ((i,(ArrayBuffer(signatures(0)), totalWeight / ECP, totalWeight)))
    signatures = signatures.drop(1)
    for(currSig <- signatures){
      totalWeight += getWgt(sig2numSZTnumMAC(currSig))
      val ratio = totalWeight / ECP
      if(ratio <= ub){
        gId2sigsRatio(i)._1 += currSig
        gId2sigsRatio(i) = (gId2sigsRatio(i)._1, ratio, totalWeight)
      }else{

        var ratio_i = gId2sigsRatio(i)._2
        if(ratio_i < lb){
          gId2sigsRatio(i)._1 += currSig +"#"+ (1 - ratio_i)
          gId2sigsRatio(i) = (gId2sigsRatio(i)._1, 1.0, gId2sigsRatio(i)._3 + ((1 - ratio_i)*ECP).toLong)


          i += 1
          val ratio_rest = getWgt(sig2numSZTnumMAC(currSig))/ECP - (1 - ratio_i)
          totalWeight = (ratio_rest * ECP).toLong
          gId2sigsRatio += ((i,(ArrayBuffer(currSig + "#" + ratio_rest), ratio_rest,totalWeight)))
        }
        else if(ratio_i > ub){
          val sig = gId2sigsRatio(i)._1(0).split("#")(0)
          gId2sigsRatio(i) = (ArrayBuffer(sig + "#"+ 1.0),1.0, (1.0 * ECP).toLong)
          ratio_i -= 1
          while(ratio_i > ub){
            i += 1
            gId2sigsRatio += ((i,(ArrayBuffer(sig + "#"+ 1.0),1.0,(1.0 * ECP).toLong)))
            ratio_i -= 1
          }
          i += 1
          gId2sigsRatio += ((i,(ArrayBuffer(sig + "#" + ratio_i),ratio_i,(ratio_i * ECP).toLong)))
          if(ratio_i >= lb){
            i += 1
            totalWeight = getWgt(sig2numSZTnumMAC(currSig))
            gId2sigsRatio += ((i,(ArrayBuffer(currSig),totalWeight / ECP,totalWeight)))
          }else{ //ratio_i < lb
            totalWeight = (ratio_i * ECP).toLong + getWgt(sig2numSZTnumMAC(currSig))
            if(totalWeight / ECP > ub){
              gId2sigsRatio(i)._1 += currSig +"#"+ (1 - ratio_i)
              gId2sigsRatio(i) = (gId2sigsRatio(i)._1, 1.0,  gId2sigsRatio(i)._3 + ((1 - ratio_i) * ECP).toLong)

              i += 1
              val ratio_rest = getWgt(sig2numSZTnumMAC(currSig))/ECP - (1 - ratio_i)
              totalWeight = (ratio_rest * ECP).toLong
              gId2sigsRatio += ((i, (ArrayBuffer(currSig + "#" + ratio_rest),ratio_rest, totalWeight)))
            }else{
              gId2sigsRatio(i)._1 += currSig
              gId2sigsRatio(i) = (gId2sigsRatio(i)._1, totalWeight / ECP, totalWeight)
            }
          }
        }//end if ratio_i > ub
        else {
          i += 1
          totalWeight = getWgt(sig2numSZTnumMAC(currSig))
          gId2sigsRatio += ((i,(ArrayBuffer(currSig),totalWeight / ECP, totalWeight)))
        }//end else lb ≤ ratio_i ≤ ub
      }//end  //ratio > ub
    }//end for
    gId2sigsRatio.toArray
  }//end func

}
