package utils

import org.slf4j.LoggerFactory
import utils.SQLOper.DBType.DBType

import java.sql.{Connection, DriverManager, ResultSet}

/**
  * sql db operator
  */
object SQLOper {

  val logger = LoggerFactory.getLogger(SQLOper.getClass)

  object DBType extends Enumeration {
    type DBType = Value
    val postgresql = Value
  }

  private val DB_SUFFIX_DRIVER: Map[DBType, (String, String)] = Map(
    DBType.postgresql -> ("jdbc:postgresql://", "org.postgresql.Driver")
  )

  /**
    * build pg connection
    * @param host
    * @param port
    * @param user
    * @param pwd
    * @param db
    * @return
    */
  def conn(
      host: Option[AnyRef],
      port: Option[AnyRef],
      user: Option[AnyRef],
      pwd: Option[AnyRef],
      db: Option[AnyRef],
      dbType: DBType
  ): Connection = {
    val conn_str =
      if (
        host
          .getOrElse("")
          .toString
          .startsWith(DB_SUFFIX_DRIVER.getOrElse(dbType, ("", ""))._1)
      ) {
        host.getOrElse("").toString
      } else {
        s"${DB_SUFFIX_DRIVER.getOrElse(dbType, ("", ""))._1}${host.getOrElse("")}:${port
          .getOrElse("")}/${db.getOrElse("")}"
      }
    Class
      .forName(DB_SUFFIX_DRIVER.getOrElse(dbType, ("", ""))._2)
      .newInstance
    logger.info("connecting with {}", conn_str)
    if (user.isEmpty)
      DriverManager.getConnection(conn_str)
    else
      DriverManager.getConnection(
        conn_str,
        user.getOrElse("").toString,
        pwd.getOrElse("").toString
      )
  }

  def runQuery(sql: String)(implicit conn: Connection): ResultSet = {
    val statement = conn.createStatement()
    statement.executeQuery(sql)
  }

  /**
    * run one sql
    * @param sql
    * @param conn
    * @return
    */
  def runSQL(sql: String)(implicit conn: Connection) = {
    logger.debug(s"running sql: ${sql}")
    val statement = conn.createStatement()
    statement.execute(sql)
  }

  /**
    * run multi sql in one transaction
    * @param sql
    * @param conn
    * @return
    */
  def runMultiSQL(
      sql: List[String]
  )(implicit conn: Connection): Boolean = {
    logger.info("will run {} sqls now", sql.size)
    try {
      conn.setAutoCommit(false)
      val statement = conn.createStatement()
      sql.foreach(x => {
//        logger.debug("batch running sql:{}", x);
        statement.execute(x)
      })
      conn.commit()
      conn.setAutoCommit(true)
      logger.info("batch committed")
      true
    } catch {
      case ex: Exception => {
        logger.error("failed to batch run sql", ex)
        conn.rollback()
        false
      }
    }
  }
}
