package MinHash

import org.apache.spark.rdd.RDD
import scala.collection.mutable.ListBuffer

class LSHModel(timelocCount:Int, numHashFunc:Int, numHashTables:Int) extends Serializable {

  private val _hashFunctions = ListBuffer[Hasher]()

  for (_ <- 0 until numHashFunc * numHashTables)
    _hashFunctions += Hasher.create(timelocCount)

  final var hashFunctions: List[(Hasher, Int)] = _hashFunctions.toList.zipWithIndex

  var sztHashTables: RDD[( String, Iterable[String])] = _
  var macHashTables: RDD[( String, Iterable[String])] = _

  var macHashRow: RDD[(String, Iterable[String])] = _

  //fusionHashTables --- mac Join szt
  var hashTables:  RDD[(Iterable[String], Iterable[String])] = _

}