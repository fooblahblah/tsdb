package tsdb

import akka.util.Timeout
import ch.systemsx.cisd.hdf5._
import scala.collection.JavaConversions._
import scala.reflect._
import org.joda.time._
import java.util.concurrent.atomic.AtomicLong
import scala.collection.mutable.ArrayBuffer
import cache.ExpirationPolicy
import cache.Stage
import java.util.concurrent.TimeUnit
import scalaz._
import Scalaz._

class TSDB(val fileName: String) {
  private val writer    = HDF5Factory.open(fileName)
  private val compounds = writer.compounds()
  private val entryType = compounds.getInferredType(classOf[Entry])

  private val expiration = ExpirationPolicy(Some(100), Some(0), TimeUnit.MILLISECONDS)
  private val stage      = Stage[String, List[Entry]](expiration, 100000)(evict)

  /**
   *  Callback to evict and flush via writer
   */
  def evict(key: String, entries: List[Entry]) {
    println(s"evicting ${entries.length} to $key")
    val offset = 0
    writer.writeCompoundArrayBlockWithOffset(key, entryType, entries.toArray, offset)
    println("evict complete")
  }


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
//      writer.setDataSetSize(path, TSDB.SECONDS_PER_DAY * 2) // This is how you grow the array
  }

  def stop {
    stage.stop(Timeout(5, TimeUnit.SECONDS))
    writer.close
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

