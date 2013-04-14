package tsdb

import com.typesafe.config.Config
import org.joda.time._
import org.voltdb._
import org.voltdb.client._
import scala.collection.JavaConversions._
import scala.annotation.tailrec
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import scalaz._
import Scalaz._
import scala.concurrent.Promise
import scala.util.Try

class TSDB(config: Config) {
  type StageEntry = (Long, Double)

  import TSDB._

  private val host  = config.getString("server.host")
  private val port  = config.getInt("server.port")
  private val seeds = config.getString("server.seeds")

  private val client = ClientFactory.createClient()
  client.createConnection(host)

  /**
   * Write a value to the path based on the day calculated from the timestamp. Each day node
   * has 86400 entries, 1 for each second of the day. Entries are written into their respective
   * subPath by day then the second slot corresponding to the seconds of the day for the timestamp.
   */
  def write(metric: String, timestamp: Long, value: Double): Future[Unit] = {
    val ts  = new DateTime(timestamp).withMillisOfSecond(0).getMillis

    val callPromise = Promise[Unit]()

    val callback = new ProcedureCallback {
      def clientCallback(response: ClientResponse) {
        callPromise.success()
      }
    }

    client.callProcedure(callback, "Upsert", metric.asInstanceOf[Object], ts.asInstanceOf[Object], value.asInstanceOf[Object])

    callPromise.future
  }


  def read(metrics: Seq[String], _start: Long, _end: Long): Future[Map[String,List[Entry]]] = {
    assert(_start <= _end)

    val start = new DateTime(_start).withMillisOfSecond(0).getMillis
    val end   = new DateTime(_end).withMillisOfSecond(0).getMillis

    val callPromise  = Promise[Map[String, List[Entry]]]()

    val stmtFutures  = metrics map { metric =>
      val promise = Promise[Seq[Entry]]()

      val callback = new ProcedureCallback {

        def clientCallback(response: ClientResponse) {
          val results = response.getResults()

          val entries = if(results.length == 0 || results(0).getRowCount() < 1) {
            Nil
          } else {
            val resultTable = results(0)
            val numRows     = resultTable.getRowCount()

            (0 until numRows) map { i =>
              resultTable.advanceRow()
              Entry(resultTable.getString("metric"), resultTable.getLong("time"), Some(resultTable.getDouble("value")))
            }
          }

          promise.success(entries)
        }
      }

      client.callProcedure(callback, "Find", metric.asInstanceOf[Object], start.asInstanceOf[Object], end.asInstanceOf[Object])
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


  private [tsdb] def truncateTimeseries() {
    client.callProcedure("Delete")
  }
}

object TSDB {
  val MILLIS_PER_SECOND = 1000
  val MILLIS_PER_MINUTE = MILLIS_PER_SECOND * 60
  val MILLIS_PER_HOUR   = MILLIS_PER_MINUTE * 60
  val MILLIS_PER_DAY    = SECONDS_PER_DAY * 1000
  val SECONDS_PER_DAY   = 86400

  def apply(config: Config) = new TSDB(config)
}


case class Entry(metric: String, timestamp: Long, value: Option[Double])


object Implicits {
  implicit def jodaToMillis(d: DateTime): Long = d.withMillisOfSecond(0).getMillis
  implicit def jodaToMillis(d: DateMidnight): Long = new DateTime(d.getMillis).withMillisOfSecond(0).getMillis
}