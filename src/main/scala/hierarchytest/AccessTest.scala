package hierarchytest

import hierarchytest.FabricateData.{fabricateData, logger}
import org.slf4j.LoggerFactory
import utils.SQLOper

import java.sql.Connection

object AccessTest {
  val logger = LoggerFactory.getLogger(AccessTest.getClass)

  def main(args: Array[String]): Unit = {
    logger.info("application start")
    logger.info("application finished")
  }

  def batchReadTest(): Unit = {
    logger.info("start to test")
    val start = System.currentTimeMillis()
    implicit val conn = Conn.initPGConn()

    logger.info(
      "finished to test in {}s",
      (System.currentTimeMillis() - start) / 1000.0
    )
  }

  def getOneSubGroup(level: Int)(implicit conn: Connection): Unit = {
    val sql =
      s"""select "Id" from public."Hierarchy" where "PathLevel"=${level} limit 1"""
    val id = SQLOper.runQuery(sql).getInt(0)
    val sql1=s"""select * from """
  }
}
