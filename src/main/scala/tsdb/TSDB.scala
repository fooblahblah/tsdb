package tsdb

import akka.util.Timeout
import ch.systemsx.cisd.hdf5._
import scala.collection.JavaConversions._
import scala.reflect._
//import org.joda.time._
import cache.Stage
import java.util.concurrent.TimeUnit
import scalaz._
import Scalaz._
import scala.concurrent._
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import java.util.concurrent.ConcurrentHashMap
import cache.SimpleStage

class TSDB(val fileName: String) {
  private val writer    = HDF5Factory.open(fileName)
  private val compounds = writer.compounds()
  private val entryType = compounds.getInferredType(classOf[Entry])

  private val stage = SimpleStage[String, List[Entry]](Duration(250, TimeUnit.MILLISECONDS), atCapacity, evict)

  val metricOffsets = new ConcurrentHashMap[String, Long]()

  /**
   *  Callback to evict and flush via writer
   */
  def evict(path: String, entries: List[Entry]) {
    val offset = metricOffsets.get(path)
    println(s"($offset) evict $path ${entries.length}")
    writer.writeCompoundArrayBlockWithOffset(path, entryType, entries.toArray, offset)
    metricOffsets.replace(path, offset, offset + entries.length)
  }

  def combine(key: String, old: List[Entry], update: List[Entry]) = old ++ update

  def atCapacity(entries: List[Entry]) = entries.length >= 99

  /**
   * Write an entry to the given path storing an index (if the time is on the minute boundary).
   * If the entry's timestamp is not greater than the last recorded time an error is thrown.
   */
  def write(path: String, timestamp: Long, value: Double) {
    initializePathAndOffset(path)
    stage.put(path, List(Entry(timestamp, value)))
  }


  def initializePathAndOffset(path: String) {
    if(!writer.exists(path)) {
      compounds.createArray(path, entryType, TSDB.SECONDS_PER_DAY)
    }

    metricOffsets.putIfAbsent(path, 0)

//      writer.setDataSetSize(path, TSDB.SECONDS_PER_DAY * 2) // This is how you grow the array
  }

  def stop = {
    for {
      _ <- stage.stop
      f <- Future.successful(writer.close)
    } yield f
  }
}

object TSDB {
  val SECONDS_PER_DAY = 86400

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

