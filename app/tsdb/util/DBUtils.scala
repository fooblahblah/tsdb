package tsdb.util

import java.sql.Connection
import play.api.Play.current
import play.api.db.DB
import scala.util.{Try, Success, Failure }

object DBUtils {
  def withTransaction[A](name: String)(block: Connection => A): A = {
    val connection = DB.getDataSource(name).getConnection()

    try {
      Try {
        connection.setAutoCommit(false)
        val r = block(connection)
        connection.commit()
        r
      } match {
        case Failure(e) =>
          connection.rollback()
          throw e
        case Success(r) => r
      }
    } finally {
      connection.close()
    }
  }
}