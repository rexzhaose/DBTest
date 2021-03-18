package utils

import java.io.{FileWriter, PrintWriter, StringWriter}
import scala.util.Random

object Funcs {

  /**
   * get random string
   */
  val strList = "1234567890abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
  val strSize = strList.size
  def getRandomString(len: Int): String = {
    val s = new StringBuilder
    for (_ <- 1 to len) {
      s += strList(Random.nextInt(strSize))
    }
    s.toString()
  }

  /**
    * convert unicode start with '\\u' to @#@$@$@4
    *
    * @param unicode
    * @return
    */
  def unicodeToCn(unicode: String) = {
    val strs = unicode.split("\\\\u")
    var returnStr = strs(0)
    for (i <- 1 until strs.length) {
      println(strs(i).substring(0, 4))
      returnStr += Integer
        .valueOf(strs(i).substring(0, 4), 16)
        .intValue
        .toChar + strs(i).substring(4)
    }
    returnStr
  }

  /**
    * write exception data to log file
    * @param e
    * @param data
    * @param outputFile
    */
  def printStackTraceToFile(e: Exception, data: String, outputFile: String) = {
    val sw: StringWriter = new StringWriter()
    val pw: PrintWriter = new PrintWriter(sw)
    e.printStackTrace(pw)
    val fw: FileWriter = new FileWriter(outputFile)
    fw.write(s"${sw.toString}\r\nData:${data}\r\n")
    pw.flush()
    sw.flush()
    fw.flush()
    pw.close()
    sw.close()
    fw.close()
  }
}
