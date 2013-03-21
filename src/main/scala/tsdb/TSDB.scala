package tsdb

import ch.systemsx.cisd.hdf5._
import scala.collection.JavaConversions._
import scala.reflect._
import org.joda.time._
import java.util.concurrent.atomic.AtomicLong
import scala.collection.mutable.ArrayBuffer
import net.sf.ehcache._

class TSDB(val fileName: String) {
  private val writer    = HDF5Factory.open(fileName)
  private val compounds = writer.compounds()
  private val entryType = compounds.getInferredType(classOf[Entry])

  private val bufferSize = 1000

  val offset = new AtomicLong(-1)
  val buffer = new ArrayBuffer[Entry](bufferSize)


  /**
   * Write an entry to the given path storing an index (if the time is on the minute boundary).
   * If the entry's timestamp is not greater than the last recorded time an error is thrown.
   */
  def write(path: String, timestamp: Long, value: Double) {
    initializePathAndOffset(path)

    if(buffer.size < bufferSize) buffer.append(Entry(timestamp, value))
    else {
      // Append the array block from the buffer
      writer.writeCompoundArrayBlockWithOffset(path, entryType, buffer.toArray, offset.getAndAdd(bufferSize))
      buffer.clear
    }
  }

  def initializePathAndOffset(path: String) {
    if(!writer.exists(path)) {
      compounds.createArray(path, entryType, TSDB.SECONDS_PER_DAY)
      offset.set(0)
    } else if(offset.get == -1) {
      writer.setDataSetSize(path, TSDB.SECONDS_PER_DAY * 2)
      offset.set(TSDB.SECONDS_PER_DAY)
      println(writer.getDataSetInformation(path).getNumberOfElements())
    }
  }

  def close {
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

