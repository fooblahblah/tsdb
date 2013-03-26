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
//      skipped("disable")
      val v = Math.random() * 100
      db.write(path, start, Math.random() * 100)
      db.write(path, start.plusSeconds(1), Math.random() * 100)
      db.write(path, start.plusSeconds(2), Math.random() * 100)
      db.write(path, start.plusSeconds(3), Math.random() * 100)
      db.write(path, start.plusSeconds(4), v)

      db.read(path, start, start.plusSeconds(4)).lastOption.flatMap(_.value) must eventually(5, new Duration(500))(beSome(v))
    }

    db.read(path, start, start.plusSeconds(4)).length === 5
    "read gappy data" in {
//      skipped("disable")
      val v = Math.random() * 100
      db.write(path, start.plusSeconds(10).getMillis, v)
      db.read(path, start, start.plusSeconds(10)).lastOption.flatMap(_.value) must eventually(5, new Duration(500))(beSome(v))
      db.read(path, start, start.plusSeconds(10)).length === 11
    }

    "read/write day boundaries" in {
//      skipped("disable")
      val v = Math.random() * 100
      db.write(path, start.plusSeconds(86399).getMillis, Math.random() * 100)
      db.write(path, start.plusSeconds(86400).getMillis, Math.random() * 100)
      db.write(path, start.plusSeconds(86401).getMillis, v)

      val f = () => {
        val r = db.read(path, start.plusSeconds(86399), start.plusSeconds(86401))
//        println(r)
        r
      }

      f().lastOption.flatMap(_.value) must eventually(5, new Duration(500))(beSome(v))
      f().length === 3
    }

    "read ranges outside bounds" in {
//      skipped("disable")
      val v = Math.random() * 100
      db.write(path, start, v)

      val f = () => {
        val r = db.read(path, start.minusHours(1), start.plusSeconds(0))
//        println(r)
        r
      }
      f().lastOption.flatMap(_.value) must eventually(5, new Duration(500))(beSome(v))
    }

    "read day of data" in {
      db.read(path, start, start.plusHours(24)).length must eventually(5, new Duration(500))(be_==(86401))
    }
    step {
      db.stop map { _ =>
        println("DB shutdown successfully")
      }
    }
  }
}
