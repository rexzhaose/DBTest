package utils

import scala.util.Random

/**
 * only support positive integer/long now
 */
object CompressEncoding {

  val baseChar =
    "1234567890abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
  val base = baseChar.size

  def long2Str(input: Long): String = {
    val res = new StringBuilder
    var d = input
    while (d > 0) {
      res.insert(0, baseChar((d % base).toInt))
      d = d / base
    }
    res.toString
  }

  def str2Long(input: String): Long = {
    var res = 0L
    var s = input
    while (s.nonEmpty) {
      res = res * base + baseChar.indexOf(s(0))
      s = s.drop(1)
    }
    res
  }

  def main(args: Array[String]): Unit = {
    val t = (1 to 50).map(_ => {
      val r = Math.abs(Random.nextLong())
      val s = long2Str(r)
      val r2 = str2Long(s)
      (r, s, r2, r == r2)
    })
    println(t.mkString("\r\n"))
  }
}
