package tsdb

import akka.util.Timeout
import ch.systemsx.cisd.hdf5._
import scala.collection.JavaConversions._
import scala.reflect._
import cache.Stage
import java.util.concurrent.TimeUnit
import scalaz._
import Scalaz._
import scala.concurrent._
import scala.concurrent.duration.Duration
import scala.concurrent.ExecutionContext.Implicits.global
import cache.SimpleStage
import ch.systemsx.cisd.hdf5.IHDF5WriterConfigurator.SyncMode
import org.joda.time._
import scala.annotation.tailrec
import org.joda.time.Days

class TSDB(val fileName: String) {
  import TSDB._

  private val writer    = HDF5Factory.open(fileName)
  private val compounds = writer.compounds()
  private val entryType = compounds.getInferredType(classOf[InternalEntry])

  private val stage = SimpleStage[String, List[InternalEntry]](Duration(250, TimeUnit.MILLISECONDS), atCapacity, evict)


  /**
   * Write a value to the path based on the day calculated from the timestamp. Each day node
   * has 86400 entries, 1 for each second of the day. Entries are written into their respective
   * subPath by day then the second slot corresponding to the seconds of the day for the timestamp.
   */
  def write(path: String, timestamp: Long, value: Double) {
    stage.put(path, List(InternalEntry(new DateTime(timestamp).withMillisOfSecond(0).getMillis, value)))
  }


  def read(path: String, start: Long, end: Long): List[Entry] = {
    assert(start <= end)

//    @tailrec
    def readSegment(start: Long, remaining: Int): List[Entry] = {
      val offset    =  new DateTime(start).secondOfDay().get
      val startPath = s"$path/${new DateTime(start).withMillisOfDay(0).getMillis}"

      def foldEntries(entries: Seq[InternalEntry]): List[Entry] = {
        entries.foldLeft((0, List[Entry]())) { (acc, e) =>
          val (i, es) = acc

          val entry = if(e.timestamp == 0) {
            Entry(new DateTime(start).plusSeconds(i).getMillis, None)
          } else Entry(e.timestamp, Some(e.value))

          (i + 1, es :+ entry)
        }._2
      }

      val diff = SECONDS_PER_DAY - offset

      if(remaining <= diff)
        foldEntries(readRange(startPath, remaining, offset))
      else
        foldEntries(readRange(startPath, diff, offset)) ++
          readSegment(new DateTime(start).plusSeconds(diff).getMillis, remaining - diff)
    }

    val secondsBetween = Seconds.secondsBetween(new DateTime(start).withMillisOfSecond(0), new DateTime(end).withMillisOfSecond(0)).getSeconds
    readSegment(start, secondsBetween + 1)
  }


  def stop = {
    for {
      _ <- stage.stop
      f <- Future.successful(writer.close)
    } yield f
  }


  /**
   *  Callback to evict and flush via writer. For each entry, the day is calculated and the
   *  entries are grouped by day. Entries are written to their respective second slots in each day.
   */
  private def evict(path: String, evicted: List[InternalEntry]) {
    evicted.sortBy(_.timestamp).groupBy(e => new DateTime(e.timestamp).withMillisOfDay(0).getMillis) foreach { kv =>
      val subPath = s"$path/${kv._1}"
      if(!writer.exists(subPath)) compounds.createArray(subPath, entryType, SECONDS_PER_DAY)

      kv._2.foreach { e =>
        val offset = new DateTime(e.timestamp).secondOfDay().get
        writer.writeCompoundArrayBlockWithOffset(subPath, entryType, Array(e), offset)
      }
    }

    writer.flush()
  }


  // Determines whether it's time to flush based on capacity
  private def atCapacity(entries: List[InternalEntry]) = entries.length >= 128


  /**
   * Reads a range of InteralEntries or generates place holders
   */
  private def readRange(path: String, count: Int, offset: Long): List[InternalEntry] = {
    if(!writer.exists(path))
        (1 to count) map (_ => InternalEntry(0, 0)) toList
     else
        compounds.readArrayBlockWithOffset(path, entryType, count, offset).toList
  }
}

object TSDB {
  val MILLIS_PER_SECOND = 1000
  val MILLIS_PER_MINUTE = MILLIS_PER_SECOND * 60
  val MILLIS_PER_HOUR   = MILLIS_PER_MINUTE * 60
  val MILLIS_PER_DAY    = SECONDS_PER_DAY * 1000
  val SECONDS_PER_DAY   = 86400

  def apply(fileName: String) = new TSDB(fileName)
}


case class Entry(timestamp: Long, value: Option[Double])

/**
 * Value object representing an entry in the TSDB
 */
class InternalEntry {
  @BeanProperty
  var timestamp: Long = -1

  @BeanProperty
  var value: Double = 0

  override def toString() = {
    s"($timestamp, $value)"
  }
}

object InternalEntry {
  def apply(_timestamp: Long, _value: Double) = {
    val e = new InternalEntry
    e.timestamp = _timestamp
    e.value = _value
    e
  }
}

object Implicits {
  implicit def jodaToMillis(d: DateTime): Long = d.withMillisOfSecond(0).getMillis
  implicit def jodaToMillis(d: DateMidnight): Long = new DateTime(d.getMillis).withMillisOfSecond(0).getMillis
}