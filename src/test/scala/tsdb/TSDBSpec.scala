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
  println("init")

  "TSDB" should {
    "write data" in {
      db.write(path, start, Math.random() * 100)
      db.write(path, start.plusSeconds(1), Math.random() * 100)
      db.write(path, start.plusSeconds(2), Math.random() * 100)
      db.read(path, start, start.plusSeconds(2)).length must eventually(5, new Duration(500))(be_==(3))
    }

    "Read basic range" in {
      db.read(path, start, start.plusHours(1)).length must eventually(5, new Duration(500))(be_==(3))
    }

    step {
      db.stop map { _ =>
        println("DB shutdown successfully")
      }
    }
  }
}
