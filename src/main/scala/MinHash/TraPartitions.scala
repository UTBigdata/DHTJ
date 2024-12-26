package MinHash

import org.apache.spark.Partitioner

class TraPartitions(numParts: Int) extends Partitioner{
  override def numPartitions: Int = numParts

  override def getPartition(key: Any): Int = {
    key.toString.split("#")(1).toInt
  }

}