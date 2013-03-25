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
import org.joda.time.DateMidnight


@RunWith(classOf[JUnitRunner])
class TSDBSpec extends Specification {
  sequential

  val dbPath = "/tmp/tsdb.h5"
  new File(dbPath).delete

  val db    = new TSDB(dbPath)
  val path  = "stats_counts/site/web_traffic/impression"
  val start = new DateTime(new DateMidnight())

  "TSDB" should {
    "write a day's worth of data" in {
      skipped("disable")
      val begin = System.currentTimeMillis()

      0 until 86400 foreach { i =>
        db.write(path, start.plusSeconds(i), Math.random() * 100)
      }

      println(s"elapsed = ${System.currentTimeMillis() - begin}")
    }

    "read first 5 data points" in {
      db.write(path, start, Math.random() * 100)
      db.write(path, start.plusSeconds(1), Math.random() * 100)
      db.write(path, start.plusSeconds(2), Math.random() * 100)
      db.write(path, start.plusSeconds(3), Math.random() * 100)
      db.write(path, start.plusSeconds(4), Math.random() * 100)

      db.read(path, start, start.plusSeconds(4)).length must eventually(5, new Duration(500))(be_==(5))
    }

    "read gappy data" in {
      val v = Math.random() * 100
      db.write(path, start.plusSeconds(10).getMillis, v)
      db.read(path, start, start.plusSeconds(10)).lastOption.map(_.value) must eventually(5, new Duration(500))(beSome(v))
    }

    "read ranges outside bounds" in {
//      db.read(path, start.minusHours(1), start.plusHours(1)).length must eventually(5, new Duration(500))(be_==(3601))
      println(db.read(path, start, start.plusMinutes(10)))
      db.read(path, start, start.plusHours(1)).length must eventually(5, new Duration(500))(be_==(3601))
    }

    step {
      db.stop map { _ =>
        println("DB shutdown successfully")
      }
    }
  }
}
