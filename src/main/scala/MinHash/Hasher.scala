package MinHash


import scala.util.Random

/**
  * simple hashing function. defined by ints a, b, p, m
  * where a and b are seeds with a > 0.
  * p is a prime number, >= u (largest item in the universe)
  * m is the number of hash bins
  */

class Hasher(a : Int, b : Int, p : Int) extends Serializable {

  override def toString(): String = "(" + a + ", " + b + "," + p + ")";

  def hash(x: Int): Int = {
      (((a.longValue * x) + b) % p).intValue
  }

  def minhash(v: String): Int = {
    v.split("->").map { i => hash(i.toInt) }.min
  }

}

object Hasher {
  /** create a new instance providing p and m. a and b random numbers mod p */
  def create(p : Int) = new Hasher(a(p), b(p), p)

  /** create a seed "a" */
  def a(p : Int) : Int = {
    val r = new Random().nextInt(p)
    if(r == 0) //函数y=a*x+b，a不能等于0
      a(p)
    else
      r
  }

  /** create a seed "b" */
  def b(p : Int) : Int = {
    new Random().nextInt(p)
  }

}