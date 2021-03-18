package hierarchytest

import org.slf4j.LoggerFactory
import utils.{CompressEncoding, Funcs, SQLOper}

import java.sql.Connection
import scala.collection.mutable.ListBuffer
import scala.util.Random

/**
  * hierarchy function performance test in pg database
  */
object FabricateData {

  val logger = LoggerFactory.getLogger(FabricateData.getClass)

  def main(args: Array[String]): Unit = {
    logger.info("application start")
    fabricateData()
    logger.info("application finished")
  }

  /**
    * fake data
    */
  def fabricateData(): Unit = {
    logger.info("start to fake data")
    val start = System.currentTimeMillis()
    implicit val conn = Conn.initPGConn()
    fabricateSystemSettingData()
    fabricateDictionaryData()
    fabricateHierarchyData(
      1100,
      5,
      100000,
      3000,
      List[Int](5, 10, 5, 10),
      3,
      true
    )
    fabricateHierarchyData(
      2100,
      5,
      100000,
      3000,
      List[Int](5, 10, 5, 10),
      3,
      false
    )
    fabricateHierarchyData(
      3100,
      5,
      100000,
      3000,
      List[Int](5, 10, 5, 10),
      3,
      false
    )
    fabricateHierarchyData(
      4100,
      5,
      100000,
      3000,
      List[Int](5, 10, 5, 10),
      3,
      false
    )
    fabricateHierarchyData(
      5100,
      5,
      100000,
      3000,
      List[Int](5, 10, 5, 10),
      3,
      false
    )
    fabricateHierarchyData(
      6100,
      5,
      100000,
      3000,
      List[Int](5, 10, 5, 10),
      3,
      false
    )
    fabricateHierarchyData(
      7100,
      5,
      100000,
      3000,
      List[Int](5, 10, 5, 10),
      3,
      false
    )
    fabricateHierarchyData(
      8100,
      5,
      100000,
      3000,
      List[Int](5, 10, 5, 10),
      3,
      false
    )
    fabricateHierarchyData(
      9100,
      5,
      100000,
      3000,
      List[Int](5, 10, 5, 10),
      3,
      false
    )
    fabricateHierarchyData(
      1000,
      5,
      100000,
      3000,
      List[Int](5, 10, 5, 10),
      3,
      false
    )
    logger.info(
      "finished to fake data in {}s",
      (System.currentTimeMillis() - start) / 1000.0
    )
  }

  /**
    *
    * @param conn
    */
  def fabricateSystemSettingData()(implicit conn: Connection): Unit = {
    logger.info("start to fake systemsetting data")
    val start = System.currentTimeMillis()
    val name = "hierarchyRule"
    val value =
      "{'1':['1','2'],'2':['2','3','4'],'3':['4','5'],'4':['5'],'5':[]}"
    val sql_format =
      "insert into \"SystemSetting\" values('%s','%s')"
    val sqls = List[String](
      s"""delete from "SystemSetting" where "Name"='${name}'""",
      sql_format.format(name, value.replace("'", "''"))
    )
    val status = if (SQLOper.runMultiSQL(sqls)) "finished" else "failed"
    logger.info(
      "{} to fake systemsetting data in {}s",
      status,
      (System.currentTimeMillis() - start) / 1000.0
    )
  }

  /**
    * fake dictionary table data
    * @param conn
    */
  def fabricateDictionaryData()(implicit conn: Connection): Unit = {
    logger.info("start to fake dictionary data")
    val start = System.currentTimeMillis()
    val hType = "hierarchyTypeName"
    val hTypeTable = "hierarchyTypeTable"
    val sql_format =
      "insert into \"Dictionary\" values('%s','%s','%s')"
    val sqls = List[String](
      s"""delete from "Dictionary" where "Category"='${hType}' or "Category"='${hTypeTable}'""",
      sql_format.format(hType, -1, "组织"),
      sql_format.format(hType, 1, "园区"),
      sql_format.format(hType, 2, "建筑"),
      sql_format.format(hType, 3, "楼层"),
      sql_format.format(hType, 4, "房间"),
      sql_format.format(hType, 5, "自定义"),
      sql_format.format(hTypeTable, 1, ""),
      sql_format.format(hTypeTable, 2, "Building"),
      sql_format.format(hTypeTable, 3, "")
    )
    val status = if (SQLOper.runMultiSQL(sqls)) "finished" else "failed"
    logger.info(
      "{} to fake dictionary data in {}s",
      status,
      (System.currentTimeMillis() - start) / 1000.0
    )
  }

  /**
    * fake hierarchy table data
    * @param conn
    */
  def fabricateHierarchyData(
      customerId: Int,
      maxLevel: Int,
      dataNum: Int,
      batchLimit: Int,
      levelParam: List[Int],
      jitter: Int,
      deleteOldData: Boolean
  )(implicit
      conn: Connection
  ): Unit = {
    logger.info("start to fake hierarchy data for customer {}", customerId)
    val start = System.currentTimeMillis()
    val rootCount = Math
      .ceil(
        dataNum.toDouble / levelParam
          .take(maxLevel - 1)
          .fold(1)({ (x, y) => x * y })
      )
      .toInt
    val sql_prefix =
      s"""insert into "Hierarchy"("Id","Type","ParentId","CustomerId","PathLevel","Order","Path"
         |,"Code","Name","Comment","UpdateUser","QRCode") values""".stripMargin
    val value_format =
      s"(%d,%d,%d,${customerId},%d,%d,'%s','%s','%s','%s','%s','%s')"

    val sqlList = ListBuffer[String]()
    if (deleteOldData) {
      sqlList += s"""delete from "Hierarchy""""
    }
    val totalData = ListBuffer[String]()
    var curData = {
      val f = rootCount.toString.size
      (1 to rootCount)
        .map(x => {
          val id =
            (customerId.toString + x.toString.reverse.padTo(f, '0')).toLong
          val code = CompressEncoding.long2Str(id) + "-"
          (
            value_format.format(
              id,
              1,
              -1,
              1,
              x,
              code,
              Funcs.getRandomString(10),
              Funcs.getRandomString(10),
              Funcs.getRandomString(50),
              Funcs.getRandomString(10),
              Funcs.getRandomString(30)
            ),
            id,
            code
          )
        })
        .toList
    }
    totalData ++= curData.map(_._1)
    var totalCnt = 0
    for (j <- (2 to maxLevel)) {
      curData = curData.flatMap(lastLevelData => {
        val f = (levelParam(j - 2) + jitter).toString.size
        (1 to levelParam(j - 2) + Random.nextInt(jitter * 2 + 1) - jitter).map(
          x => {
            val newId = (lastLevelData._2.toString + x.toString.reverse
              .padTo(f, '0')).toLong
            val newCode =
              lastLevelData._3 + CompressEncoding.long2Str(newId) + "-"
            (
              value_format.format(
                newId,
                Random.nextInt(maxLevel - j + 1) + j - 1,
                lastLevelData._2,
                j,
                x,
                newCode,
                Funcs.getRandomString(10),
                Funcs.getRandomString(10),
                Funcs.getRandomString(50),
                Funcs.getRandomString(10),
                Funcs.getRandomString(30)
              ),
              newId,
              newCode
            )
          }
        )
      })
      logger.debug("{} new data in level {} were generated", curData.size, j)
      totalData ++= curData.map(_._1)
      while (totalData.size > batchLimit) {
        sqlList += sql_prefix + totalData
          .take(batchLimit)
          .foldLeft("")((x, y) => { s"${x}${y}," })
          .dropRight(1)
        totalData.remove(0, batchLimit)
        totalCnt += batchLimit
      }
    }
    if (totalData.size > 0) {
      sqlList += sql_prefix + totalData
        .foldLeft("")((x, y) => {
          s"${x}${y},"
        })
        .dropRight(1)
      totalCnt += totalData.size
    }
    val status =
      if (SQLOper.runMultiSQL(sqlList.toList)) "finished" else "failed"
    logger.info(
      s"${totalCnt} fake hierarchy data for customer ${customerId} ${status} in ${(System
        .currentTimeMillis() - start) / 1000.0}s"
    )
  }
}
