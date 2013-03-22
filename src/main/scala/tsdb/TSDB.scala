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
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import java.util.concurrent.ConcurrentHashMap
import cache.SimpleStage
import ch.systemsx.cisd.hdf5.IHDF5WriterConfigurator.SyncMode

class TSDB(val fileName: String) {
  private val writer    = HDF5Factory.open(fileName)
  private val compounds = writer.compounds()
  private val entryType = compounds.getInferredType(classOf[Entry])

  private val stage = SimpleStage[String, List[Entry]](Duration(250, TimeUnit.MILLISECONDS), atCapacity, evict)

  private val metricOffsets = new ConcurrentHashMap[String, Long]()

  private val chunkSize = 4096

  /**
   *  Callback to evict and flush via writer
   */
  private def evict(path: String, entries: List[Entry]) {
    val offset = metricOffsets.get(path)
//    println(s"($offset) evict $path ${entries.length}")
    writer.writeCompoundArrayBlockWithOffset(path, entryType, entries.toArray, offset)
    metricOffsets.replace(path, offset, offset + entries.length)
  }

  // Determines whether it's time to flush based on capacity
  private def atCapacity(entries: List[Entry]) = entries.length >= 128

  private def initializePathAndOffset(path: String) {
    if(!writer.exists(path)) {
      compounds.createArray(path, entryType, TSDB.SECONDS_PER_DAY)
    }

    metricOffsets.putIfAbsent(path, 0)

//      writer.setDataSetSize(path, TSDB.SECONDS_PER_DAY * 2) // This is how you grow the array
  }


  /**
   * Write an entry to the given path storing an index (if the time is on the minute boundary).
   * If the entry's timestamp is not greater than the last recorded time an error is thrown.
   */
  def write(path: String, timestamp: Long, value: Double) {
    initializePathAndOffset(path)
    stage.put(path, List(Entry(timestamp, value)))
  }


  def read(path: String, start: Long, end: Long): List[Entry] = {
    if(writer.exists(path)) {
      val numEntries  = writer.getNumberOfElements(path) - 1

      // Ensure the start is after the actual start, otherwise just use actual start
      compounds.readArrayBlockWithOffset(path, entryType, 1, 0).headOption.map(_.timestamp).map(actual => if(actual < start) start else actual).map { start =>

        binarySearch(path, start, 0, numEntries).map { i =>
          def readChunk(offset: Long) = {
            if(offset < numEntries) {
              val remaining = numEntries - offset - chunkSize + 1
              val chunk = if(remaining < 0) Math.abs(remaining).toInt else chunkSize
              compounds.readArrayBlockWithOffset(path, entryType, chunk, offset).toList
            } else {
              Nil
            }
          }

          def generateRange(l: List[Entry]): List[Entry] = l match {
            case Nil                           => Nil
            case _ if(l.last.timestamp) > end  => l.reverse.dropWhile(_.timestamp > end).reverse
            case _                             =>
              readChunk(l.length) match {
                case Nil => l
                case xs  => generateRange(l ++ xs)
              }
          }

          generateRange(readChunk(i))

        }.getOrElse(Nil)
      }.getOrElse(Nil)
    } else Nil
  }


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


  def stop = {
    for {
      _ <- stage.stop
      f <- Future.successful(writer.close)
    } yield f
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

