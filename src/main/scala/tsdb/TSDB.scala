package tsdb

import cache.SimpleStage
import com.netflix.astyanax.AstyanaxContext
import com.netflix.astyanax.connectionpool.NodeDiscoveryType
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl
import com.netflix.astyanax.model.ColumnFamily
import com.netflix.astyanax.serializers.LongSerializer
import com.netflix.astyanax.serializers.StringSerializer
import com.netflix.astyanax.thrift.ThriftFamilyFactory
import com.typesafe.config.Config
import org.joda.time._
import scala.collection.JavaConversions._
import scala.annotation.tailrec
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import scalaz._
import Scalaz._
import scala.concurrent.Promise

class TSDB(config: Config) {
  type StageEntry = (Long, Double)

  import TSDB._

  private val host  = config.getString("server.host")
  private val port  = config.getInt("server.port")
  private val seeds = config.getString("server.seeds")

  private val context = new AstyanaxContext.Builder()
    .forCluster("Test Cluster")
    .forKeyspace("ts_1")
    .withAstyanaxConfiguration(new AstyanaxConfigurationImpl()
        .setDiscoveryType(NodeDiscoveryType.RING_DESCRIBE)
    )
    .withConnectionPoolConfiguration(new ConnectionPoolConfigurationImpl("MyConnectionPool")
        .setPort(port)
        .setMaxConnsPerHost(1)
        .setSeeds(seeds)
    )
    .withConnectionPoolMonitor(new CountingConnectionPoolMonitor())
    .withAstyanaxConfiguration(new AstyanaxConfigurationImpl()
        .setCqlVersion("3.0.0")
        .setTargetCassandraVersion("1.2")
    ) .buildKeyspace(ThriftFamilyFactory.getInstance())
  context.start()

  private val keyspace = context.getEntity()

  private val TS_CF = ColumnFamily.newColumnFamily(
      keyspace.getKeyspaceName(),
      StringSerializer.get(),
      LongSerializer.get())


  /**
   * Write a value to the path based on the day calculated from the timestamp. Each day node
   * has 86400 entries, 1 for each second of the day. Entries are written into their respective
   * subPath by day then the second slot corresponding to the seconds of the day for the timestamp.
   */
  def write(metric: String, timestamp: Long, value: Double): Future[Int] = {
    val day = new DateMidnight(timestamp).getMillis
    val id  = s"$metric:$day"
    val ts  = new DateTime(timestamp).withMillisOfSecond(0).getMillis

    val query = """INSERT INTO timeseries (id, time, value) VALUES (?, ?, ?);"""

    Future {
      keyspace
        .prepareQuery(TS_CF)
        .withCql(query)
        .asPreparedStatement()
        .withStringValue(id)
        .withLongValue(ts)
        .withDoubleValue(value)
        .executeAsync()
        .get().getResult().getNumber()
    }
  }


  def read(metric: String, _start: Long, _end: Long): Future[List[Entry]] = {
    assert(_start <= _end)

    // Normalize to seconds (chopping millis granularity)
    val start = new DateTime(_start).withMillisOfSecond(0).getMillis
    val end   = new DateTime(_end).withMillisOfSecond(0).getMillis

    @tailrec
    def dayRangeMillis(start: Long, remaining: Long, days: List[Long] = Nil): List[Long] = {
      if(remaining > 0) {
        val startDate = new DateTime(start)
        val diff      = SECONDS_PER_DAY - startDate.secondOfDay.get
        val offset    = (diff * MILLIS_PER_SECOND)
        dayRangeMillis(start + offset, remaining - offset, days :+ new DateMidnight(startDate).getMillis)
      } else days
    }

    dayRangeMillis(start, end - start).foldLeft(Future.successful(List[Entry]())) { (acc, rowTime) =>
      acc.flatMap(l => readDateRange(metric, rowTime, start, end).map(l ++ _))
    }
  }


  private def readDateRange(metric: String, rowTime: Long, start: Long, end: Long): Future[Seq[Entry]] = {
    val query = """SELECT time, value FROM timeseries WHERE id=? AND time >= ? AND time <= ?;"""
    val id    = s"$metric:$rowTime"


    Future {
      val entries = keyspace
        .prepareQuery(TS_CF)
        .withCql(query)
        .asPreparedStatement()
        .withStringValue(id)
        .withLongValue(start)
        .withLongValue(end)
        .executeAsync()
        .get().getResult().getRows().map { row =>
          val cols  = row.getColumns()
          val ts    = cols.getColumnByIndex(0).getLongValue()
          val value = cols.getColumnByIndex(1).getDoubleValue()
          Entry(ts, Some(value))
        }.toList

      val rowStart   = if(start > rowTime) start else rowTime
      val rowEndTime = new DateTime(rowTime).plusHours(24).minusSeconds(1).getMillis
      val rowEnd     = if(end > rowEndTime) rowEndTime else end
      expandSeries(rowStart, rowEnd, entries)
    }
  }


  private def expandSeries(start: Long, end: Long, entries: List[Entry]): List[Entry] = {
    entries.foldLeft(List[Entry]()) { (acc, e) =>
      acc.headOption.map { head  =>
        val prev = head.timestamp
        val secs = secondsBetween(prev, e.timestamp)

        val expanded = if(secs > 1) {
          (secs - 1 to 1L by -1).map(i => Entry(prev + (MILLIS_PER_SECOND * i), None)).toList
        } else Nil

        e +: (expanded ++ acc)
      }.getOrElse(e +: acc)

    } reverse match {
      case Nil =>
        val secs = secondsBetween(start, end)
        (0L to secs).map(i => Entry(start + (MILLIS_PER_SECOND * i), None)).toList

      case middle =>
        val startSecs = secondsBetween(start, middle.head.timestamp)
        val endSecs   = secondsBetween(middle.last.timestamp, end)
        val front     = (1L to startSecs).map(i => Entry(start + (MILLIS_PER_SECOND * i), None)).toList
        val tail      = (1L to endSecs).map(i => Entry(middle.last.timestamp + (MILLIS_PER_SECOND * i), None)).toList

        front ++ middle ++ tail
    }
  }


  private def secondsBetween(start: Long, end: Long): Long = {
    assert(start <= end)
    (end - start) / 1000
  }


  private def plusSeconds(start: Long, seconds: Long): Long = start + (MILLIS_PER_SECOND * seconds)


  private [tsdb] def truncateTimeseries() {
    keyspace
      .prepareQuery(TS_CF)
      .withCql("TRUNCATE timeseries")
      .execute()
  }


  def stop = {
    context.shutdown()
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


case class Entry(timestamp: Long, value: Option[Double])


object Implicits {
  implicit def jodaToMillis(d: DateTime): Long = d.withMillisOfSecond(0).getMillis
  implicit def jodaToMillis(d: DateMidnight): Long = new DateTime(d.getMillis).withMillisOfSecond(0).getMillis
}