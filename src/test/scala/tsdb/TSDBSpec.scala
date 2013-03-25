package tsdb

import Implicits._
import java.io.File
import java.util.Date
import org.joda.time.DateTime
import org.junit.runner._
import org.specs2.mutable.Specification
import org.specs2.runner._
import org.specs2.time.Duration
import scala.concurrent.ExecutionContext.Implicits.global
import java.util.concurrent.TimeUnit


@RunWith(classOf[JUnitRunner])
class TSDBSpec extends Specification {
  sequential

  val dbPath = "/tmp/tsdb.h5"
  new File(dbPath).delete

  val db    = new TSDB(dbPath)
  val path  = "stats_counts/site/web_traffic/impression"
  val start = new DateTime(System.currentTimeMillis())

  "TSDB" should {
    "write a days worth of data" in {
      0 until 86400 foreach { i =>
        db.write(path, start + (i*1000), Math.random() * 100)
      }
    }

    "read first 3 data points" in {
      db.read(path, start, start.plusSeconds(2)).length must eventually(5, new Duration(500))(be_==(3))
    }

    "read ranges outside bounds" in {
      db.read(path, start.minusHours(1), start.plusHours(1)).length must eventually(5, new Duration(500))(be_==(3601))
    }

    step {
      db.stop map { _ =>
        println("DB shutdown successfully")
      }
    }
  }
}
