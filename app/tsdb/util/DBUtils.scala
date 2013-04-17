package tsdb.util

import java.sql.Connection
import play.api.Play.current
import play.api.db.DB
import scala.util.{Try, Success, Failure }

object DBUtils {
  def withConnection[A](name: String)(block: Connection => A): A = {
    val connection = DB.getDataSource(name).getConnection()

    Try {
      connection.setAutoCommit(true)
      block(connection)
    } match {
      case Failure(e) =>
        connection.close()
        throw e

      case Success(r) =>
        connection.close()
        r
    }
  }

  def withTransaction[A](name: String)(block: Connection => A): A = {
    val connection = DB.getDataSource(name).getConnection()

    Try {
      val r = block(connection)
      connection.commit()
      r
    } match {
      case Failure(e) =>
        connection.rollback()
        connection.close()
        throw e

      case Success(r) =>
        connection.close()
        r
    }
  }
}