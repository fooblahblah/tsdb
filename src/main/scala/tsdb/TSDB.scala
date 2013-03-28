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
  def write(metric: String, timestamp: Long, value: Double) = {
    val day = new DateMidnight(timestamp).getMillis
    val id  = s"$metric:$day"
    val ts  = new DateTime(timestamp).withMillisOfSecond(0).getMillis

    val query = """INSERT INTO timeseries (id, time, value) VALUES (?, ?, ?);"""

    keyspace
      .prepareQuery(TS_CF)
      .withCql(query)
      .asPreparedStatement()
      .withStringValue(id)
      .withLongValue(ts)
      .withDoubleValue(value)
      .executeAsync()
  }


  def read(metric: String, start: Long, end: Long): Future[List[Entry]] = {
    assert(start <= end)

    @tailrec
    def dayRangeMillis(start: Long, remaining: Long, days: List[Long] = Nil): List[Long] = {
      if(remaining > 0) {
        val startDate = new DateTime(start)
        val diff      = SECONDS_PER_DAY - startDate.secondOfDay.get
        val offset    = (diff * MILLIS_PER_SECOND)
        dayRangeMillis(start + offset, remaining - offset, days :+ new DateMidnight(startDate).getMillis)
      } else days
    }

    dayRangeMillis(start, end - start).foldLeft(Future.successful(List[Entry]())) { (acc, day) =>
      acc.flatMap(l => readDateRange(metric, day, start, end).map(l ++ _))
    }.map { l =>
      expandSeries(start, end, l)
    }
  }


  private def readDateRange(metric: String, rowTime: Long, start: Long, end: Long): Future[Seq[Entry]] = {
    val query = """SELECT time, value FROM timeseries WHERE id=? AND time >= ? AND time <= ?;"""
    val id    = s"$metric:$rowTime"


    Future {
      keyspace
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
    }
  }


  def expandSeries(start: Long, end: Long, entries: List[Entry]): List[Entry] = {
    def _expander(cur: Long, l: List[Entry]) = {
      l match {
        // Pad head with missing entries
        case x :: xs if x.timestamp > start =>
          val secs = Seconds.secondsBetween(new DateTime(cur), new DateTime(x.timestamp)).getSeconds()
          val expanded = (0 until secs).map(i => Entry(cur + (MILLIS_PER_SECOND * i), None)).toList
          expanded ++ List(x) ++ expandSeries(cur + (MILLIS_PER_SECOND * secs), end, xs)

        // Fill gaps betweem entries
        case x :: xs =>
          val secs = Seconds.secondsBetween(new DateTime(cur), new DateTime(x.timestamp)).getSeconds()
          if(secs > 1) {
            val expanded = (1 until secs).map(i => Entry(cur + (MILLIS_PER_SECOND * i), None)).toList
            expanded ++ List(x) ++ expandSeries(x.timestamp + (MILLIS_PER_SECOND * secs), end, xs)
          } else
            x :: expandSeries(x.timestamp + MILLIS_PER_SECOND, end, xs)

        // Pad the end of the list
        case Nil if cur < end =>
          val secs = Seconds.secondsBetween(new DateTime(cur), new DateTime(end)).getSeconds()
          (0 until secs).map(i => Entry(cur + (MILLIS_PER_SECOND * i), None)).toList

        case Nil => Nil
      }
    }

    _expander(start, entries)
  }


  def truncateTimeseries() {
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