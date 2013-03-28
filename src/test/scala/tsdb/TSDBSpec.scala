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
import java.util.concurrent.TimeUnit._
import org.joda.time.DateMidnight
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import scala.concurrent.Await
import scala.concurrent.duration.Duration
import org.specs2.specification.BeforeExample


@RunWith(classOf[JUnitRunner])
class TSDBSpec extends Specification with BeforeExample {
  sequential

  val config = ConfigFactory.parseString("""
      server.host="localhost"
      server.port=9160
      server.seeds="localhost:9160"
   """)

  val db     = new TSDB(config)
  val metric = "stats_counts.site.web_traffic.impression"
  val start  = new DateTime(new DateMidnight())

  def before {
    db.truncateTimeseries()
  }

  "TSDB" should {
//    "write a day's worth of data" in {
//      val begin = System.currentTimeMillis()
//
//      val futures = 0 until 86400 map { i =>
//        db.write(metric, start.plusSeconds(i), Math.random() * 100)
//      }
//
//      futures.foreach(_.get())
//      println(s"elapsed = ${System.currentTimeMillis() - begin}")
//    }

    "read first 5 data points" in {
      val v = Math.random() * 100
      db.write(metric, start, Math.random() * 100)
      db.write(metric, start.plusSeconds(1), Math.random() * 100)
      db.write(metric, start.plusSeconds(2), Math.random() * 100)
      db.write(metric, start.plusSeconds(3), Math.random() * 100)
      db.write(metric, start.plusSeconds(4), v)

      val result = Await.result(db.read(metric, start, start.plusSeconds(4)), Duration(15, SECONDS))
      result.length === 5
      result.lastOption.flatMap(_.value) === Some(v)
    }

    "read gappy data" in {
      val v = Math.random() * 100
      db.write(metric, start.plusSeconds(10).getMillis, v)
      val result = Await.result(db.read(metric, start, start.plusSeconds(10)), Duration(15, SECONDS))
      result.length === 11
      result.lastOption.flatMap(_.value) === Some(v)
    }

    "read/write day boundaries" in {
      val v = Math.random() * 100
      db.write(metric, start.plusSeconds(86399).getMillis, Math.random() * 100)
      db.write(metric, start.plusSeconds(86400).getMillis, Math.random() * 100)
      db.write(metric, start.plusSeconds(86401).getMillis, v)

      val result = Await.result(db.read(metric, start.plusSeconds(86399), start.plusSeconds(86401)), Duration(15, SECONDS))
      result.length === 3
      result.lastOption.flatMap(_.value) === Some(v)
    }

    "read ranges outside bounds" in {
      val v = Math.random() * 100
      db.write(metric, start, v)

      val result = Await.result(db.read(metric, start.minusMinutes(1), start.plusSeconds(10)), Duration(15, SECONDS))
      result.length === 71
      result.drop(60).head.value === Some(v)
    }

    "read day of data" in {
      Await.result(db.read(metric, start, start.plusHours(24)), Duration(15, SECONDS)).length === 86400
    }

  }
}
