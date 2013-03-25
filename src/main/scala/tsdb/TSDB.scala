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
  private val entryType = compounds.getInferredType(classOf[Entry])

  private val stage = SimpleStage[String, List[Entry]](Duration(250, TimeUnit.MILLISECONDS), atCapacity, evict)


  /**
   * Write a value to the path based on the day calculated from the timestamp. Each day node
   * has 86400 entries, 1 for each second of the day. Entries are written into their respective
   * subPath by day then the second slot corresponding to the seconds of the day for the timestamp.
   */
  def write(path: String, timestamp: Long, value: Double) {
    stage.put(path, List(Entry(new DateTime(timestamp).withMillisOfSecond(0).getMillis, value)))
  }


  def read(path: String, start: Long, end: Long): List[Entry] = {
    assert(start <= end)

//    @tailrec
    def readSegment(start: Long, remaining: Int): List[Entry] = {
      println(s"readSegment $start")
      val offset    =  new DateTime(start).secondOfDay().get
      val startPath = s"$path/${new DateTime(start).withMillisOfDay(0).getMillis}"

      if(writer.exists(startPath)) {
        val diff = remaining - SECONDS_PER_DAY

        if(diff <= 0) compounds.readArrayBlockWithOffset(startPath, entryType, remaining, offset).toList
        else          compounds.readArrayBlockWithOffset(startPath, entryType, SECONDS_PER_DAY, offset).toList ++ readSegment(new DateTime(start).plusSeconds(SECONDS_PER_DAY).getMillis, diff)
      } else Nil
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
  private def evict(path: String, evicted: List[Entry]) {
    evicted.sortBy(_.timestamp).groupBy(e => new DateTime(e.timestamp).withMillisOfDay(0).getMillis) foreach { kv =>
      val subPath = s"$path/${kv._1}"
      if(!writer.exists(subPath)) {
        compounds.createArray(subPath, entryType, SECONDS_PER_DAY)
        compounds.writeArrayBlockWithOffset(subPath, entryType, (1 to SECONDS_PER_DAY).map(_ => Entry(-1, 0)).toArray, 0)
      }

      def writeEntries(entries: List[Entry]) {
        entries.headOption.map { e =>
          val offset = new DateTime(e.timestamp).secondOfDay().get
          writer.writeCompoundArrayBlockWithOffset(subPath, entryType, entries.toArray, offset)
        }
      }

      @tailrec
      def writeSegment(entries: List[Entry]) {
        entries.zip(entries.drop(1)).indexWhere(pair => pair._2.timestamp - pair._1.timestamp > TSDB.MILLIS_PER_SECOND) match {
          case i if(i == -1) => writeEntries(entries)

          case i =>
            val parted = entries.splitAt(i)
            writeEntries(parted._1)
            writeSegment(parted._2)
        }
      }

      writeSegment(kv._2)
      writer.flush()
    }
  }


  // Determines whether it's time to flush based on capacity
  private def atCapacity(entries: List[Entry]) = entries.length >= 128


  private def binarySearch(path: String, v: Long, low: Long, high: Long): Option[Long] = {
    def recurse(low: Long, high: Long): Option[Long] = (low + high) / 2 match {
      case _ if high < low => None
      case mid if readBlock(path, mid).timestamp > v => recurse(low, mid - 1)
      case mid if readBlock(path, mid).timestamp < v => recurse(mid + 1, high)
      case mid => Some(mid)
    }

    recurse(0, high)
  }


  private def readBlock(path: String, offset: Long): Entry = {
    compounds.readArrayBlockWithOffset(path, entryType, 1, offset).head
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


/**
 * Value object representing an entry in the TSDB
 */
class Entry {
  @BeanProperty
  var timestamp: Long = -1

  @BeanProperty
  var value: Double = 0

  override def toString() = {
    s"($timestamp, $value)"
  }
}

object Entry {
  def apply(_timestamp: Long, _value: Double) = {
    val e = new Entry
    e.timestamp = _timestamp
    e.value = _value
    e
  }
}

object Implicits {
  implicit def jodaToMillis(d: DateTime): Long = d.withMillisOfSecond(0).getMillis
  implicit def jodaToMillis(d: DateMidnight): Long = new DateTime(d.getMillis).withMillisOfSecond(0).getMillis
}