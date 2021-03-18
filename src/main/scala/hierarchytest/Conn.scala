package hierarchytest

import hierarchytest.FabricateData.logger
import utils.SQLOper
import utils.SQLOper.DBType

import java.sql.Connection

object Conn {

  /**
    * get pg connection
    * @return
    */
  def initPGConn(): Connection = {
    logger.info("initing pg connection")
    val pg_host = "localhost";
    val pg_port = "5432"
    val pg_user = "test"
    val pg_pass = "test"
    val pg_db = "testdb"
    val pg_schema = "public"
    SQLOper.conn(
      Option(pg_host),
      Option(pg_port),
      Option(pg_user),
      Option(pg_pass),
      Option(s"${pg_db}?currentSchema=${pg_schema}"),
      DBType.postgresql
    )
  }
}
