package tsdb.api

import anorm._
import anorm.SqlParser._
import com.typesafe.config.Config
import org.joda.time._
import play.api.Play.current
import play.api.db.DB
import scala.collection.JavaConversions._
import scala.annotation.tailrec
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Promise
import scala.util.{Try, Success, Failure}
import java.util.Properties
import com.nuodb.jdbc.DataSource
import tsdb.util.DBUtils
import scala.collection.mutable.ArrayBuffer

class TSDB {
  import TSDB._

  val CONSISTENT_READ = 7
  val WRITE_COMMITTED = 5
  val READ_COMMITTED  = 2


  def write(metric: String, timestamp: Long, value: Double): Future[Unit] = {
    val ts  = new DateTime(timestamp).withMillisOfSecond(0).getMillis

    Future {
      DBUtils.withTransaction { implicit conn =>
        conn.setTransactionIsolation(WRITE_COMMITTED)

        Try {
          SQL(s"""INSERT INTO timeseries values('$metric', $ts, $value)""").executeUpdate()
        } match {
          case Success(i) =>
          case Failure(e) =>
            SQL(s"""UPDATE timeseries SET value = value + $value WHERE metric = '$metric' AND time = $ts""").executeUpdate()
        }
      }
    }
  }


  def read(metrics: Seq[String], _start: Long, _end: Long): Future[Seq[(String, Seq[DataPoint])]] = {
    assert(_start <= _end)

    val start = new DateTime(_start).withMillisOfSecond(0).getMillis
    val end   = new DateTime(_end).withMillisOfSecond(0).getMillis

    val callPromise = Promise[Seq[(String, Seq[DataPoint])]]()

    val stmtFutures = metrics map { _metric =>
      Future {
        val metricClause = _metric.replaceAll("""\*""", "%")
        val query        = SQL(s"""SELECT metric, time, value FROM timeseries WHERE metric LIKE '$metricClause' AND time >= $start AND time <= $end ORDER BY time ASC""")

        DBUtils.withConnection { implicit conn =>
          conn.setTransactionIsolation(READ_COMMITTED)

          val entries = new ArrayBuffer[Entry]()
          val rs      = query.resultSet

          // Using an Anorm stream is 2x slower
          while(rs.next()) {
            entries += Entry(rs.getString("metric"), rs.getLong("time"), Some(rs.getDouble("value")))
          }

          entries groupBy(_.metric) map { kv =>
            (kv._1 -> expandSeries(start, end, kv._2))
          }
        }
      }
    }

    Future.sequence(stmtFutures) map { entries =>
      callPromise.success(entries.flatten.sortBy(_._1))
    }

    callPromise.future
  }


  private def expandSeries(start: Long, end: Long, entries: Seq[Entry]): Seq[DataPoint] = {
    entries.foldLeft(List[DataPoint]()) { (acc, e) =>
      acc.headOption map { head =>
        val prev = head.timestamp
        val secs = secondsBetween(prev, e.timestamp)

        val expanded = if(secs > 1) {
          (secs - 1 to 1L by -1).map(i => DataPoint(prev + (MILLIS_PER_SECOND * i), None)).toList
        } else Nil

        DataPoint(e.timestamp, e.value) :: (expanded ++ acc)
      } getOrElse(DataPoint(e.timestamp, e.value) +: acc)

    } reverse match {
      case Nil =>
        val secs = secondsBetween(start, end)
        (0L to secs).map(i => DataPoint(start + (MILLIS_PER_SECOND * i), None)).toList

      case middle =>
        val startSecs = secondsBetween(start, middle.head.timestamp)
        val endSecs   = secondsBetween(middle.last.timestamp, end)
        val front     = (1L to startSecs).map(i => DataPoint(start + (MILLIS_PER_SECOND * i), None)).toList
        val tail      = (1L to endSecs).map(i => DataPoint(middle.last.timestamp + (MILLIS_PER_SECOND * i), None)).toList

        front ++ middle ++ tail
    }
  }


  private def secondsBetween(start: Long, end: Long): Long = {
    assert(start <= end)
    (end - start) / 1000
  }


  private def plusSeconds(start: Long, seconds: Long): Long = start + (MILLIS_PER_SECOND * seconds)


  private [api] def truncateTimeseries() {
    DB.withConnection { implicit conn =>
      SQL("DELETE FROM timeseries").executeUpdate()
    }
  }
}

object TSDB {
  val MILLIS_PER_SECOND = 1000
  val MILLIS_PER_MINUTE = MILLIS_PER_SECOND * 60
  val MILLIS_PER_HOUR   = MILLIS_PER_MINUTE * 60
  val MILLIS_PER_DAY    = SECONDS_PER_DAY * 1000
  val SECONDS_PER_DAY   = 86400

  def apply() = new TSDB
}


case class Entry(metric: String, timestamp: Long, value: Option[Double])
case class DataPoint(timestamp: Long, value: Option[Double])


object Implicits {
  implicit def jodaToMillis(d: DateTime): Long = d.withMillisOfSecond(0).getMillis
  implicit def jodaToMillis(d: DateMidnight): Long = new DateTime(d.getMillis).withMillisOfSecond(0).getMillis
}