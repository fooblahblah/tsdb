package tsdb.api

import anorm._
import com.typesafe.config.Config
import org.joda.time._
import play.api.Play.current
import play.api.db.DB
import scala.collection.JavaConversions._
import scala.annotation.tailrec
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import scalaz._
import Scalaz._
import scala.concurrent.Promise
import scala.util.Try

class TSDB {
  import TSDB._

  /**
   * Write a value to the path based on the day calculated from the timestamp. Each day node
   * has 86400 entries, 1 for each second of the day. Entries are written into their respective
   * subPath by day then the second slot corresponding to the seconds of the day for the timestamp.
   */
  def write(metric: String, timestamp: Long, value: Double): Future[Int] = {
    val ts  = new DateTime(timestamp).withMillisOfSecond(0).getMillis

    Future {
      DB.withConnection { implicit conn =>
        SQL(s"""REPLACE timeseries VALUES ('$metric', $ts, COALESCE(SELECT value + $value FROM timeseries WHERE metric = '$metric' AND time = $ts, $value))""").executeUpdate()
      }
    }
  }


  def read(metrics: Seq[String], _start: Long, _end: Long): Future[Map[String,List[Entry]]] = {
    assert(_start <= _end)

    val start = new DateTime(_start).withMillisOfSecond(0).getMillis
    val end   = new DateTime(_end).withMillisOfSecond(0).getMillis

    val callPromise = Promise[Map[String, List[Entry]]]()

    val stmtFutures = metrics map { _metric =>
      val promise = Promise[Seq[Entry]]()

      Future {
        val metric = _metric.replaceAll("""\*""", "%")
        val query  = SQL(s"""SELECT metric, time, value FROM timeseries WHERE metric LIKE '$metric' AND time >= $start AND time <= $end ORDER BY time ASC;""")

        val entries = DB.withConnection { implicit conn =>
          query().map { row =>
            Entry(row[String]("metric"), row[Long]("time"), Some(row[Double]("value")))
          }.toList
        }

        promise.success(entries)
      }

      promise.future
    }

    Future.sequence(stmtFutures) map { entries =>
      val groupedEntries = entries.flatten.foldLeft(Map[String, List[Entry]]()) { (m, entry) =>
        if(m.contains(entry.metric))
          m.updated(entry.metric, entry :: m(entry.metric))
        else
          m + (entry.metric -> List(entry))
      } map { kv =>
        (kv._1 -> expandSeries(kv._1, start, end, kv._2.reverse))
      }

      callPromise.success(groupedEntries)
    }

    callPromise.future
  }


  private def expandSeries(metric: String, start: Long, end: Long, entries: Seq[Entry]): List[Entry] = {
    entries.foldLeft(List[Entry]()) { (acc, e) =>
      acc.headOption.map { head  =>
        val prev = head.timestamp
        val secs = secondsBetween(prev, e.timestamp)

        val expanded = if(secs > 1) {
          (secs - 1 to 1L by -1).map(i => Entry(metric, prev + (MILLIS_PER_SECOND * i), None)).toList
        } else Nil

        e +: (expanded ++ acc)
      }.getOrElse(e +: acc)

    } reverse match {
      case Nil =>
        val secs = secondsBetween(start, end)
        (0L to secs).map(i => Entry(metric, start + (MILLIS_PER_SECOND * i), None)).toList

      case middle =>
        val startSecs = secondsBetween(start, middle.head.timestamp)
        val endSecs   = secondsBetween(middle.last.timestamp, end)
        val front     = (1L to startSecs).map(i => Entry(metric, start + (MILLIS_PER_SECOND * i), None)).toList
        val tail      = (1L to endSecs).map(i => Entry(metric, middle.last.timestamp + (MILLIS_PER_SECOND * i), None)).toList

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


object Implicits {
  implicit def jodaToMillis(d: DateTime): Long = d.withMillisOfSecond(0).getMillis
  implicit def jodaToMillis(d: DateMidnight): Long = new DateTime(d.getMillis).withMillisOfSecond(0).getMillis
}