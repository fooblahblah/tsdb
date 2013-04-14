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
import scala.concurrent._
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
    "read first 5 data points" in {
      val v = Math.random() * 100
      Await.ready(Future.sequence(List(
        db.write(metric, start, Math.random() * 100),
        db.write(metric, start.plusSeconds(1), Math.random() * 100),
        db.write(metric, start.plusSeconds(2), Math.random() * 100),
        db.write(metric, start.plusSeconds(3), Math.random() * 100),
        db.write(metric, start.plusSeconds(4), v)
      )), Duration.Inf)

      val result = Await.result(db.read(List(metric), start, start.plusSeconds(4)), Duration(15, SECONDS))
//      println(result)
      result.keys.size == 1
      result(metric).length === 5
      result(metric).lastOption.flatMap(_.value) === Some(v)
    }

    "read gappy data" in {
      val v = Math.random() * 100
      Await.ready(Future.sequence(List(
        db.write(metric, start.plusSeconds(3).getMillis, v),
        db.write(metric, start.plusSeconds(6).getMillis, v),
        db.write(metric, start.plusSeconds(10).getMillis, v)
      )), Duration.Inf)

      val result = Await.result(db.read(Seq(metric), start, start.plusSeconds(10)), Duration(15, SECONDS))
//      println(result)
      result.keys.size === 1
      result(metric).size === 11
      result(metric).lastOption.flatMap(_.value) === Some(v)
    }

    "read/write day boundaries" in {
      val v = Math.random() * 100
      Await.ready(Future.sequence(List(
          db.write(metric, start.plusSeconds(86399).getMillis, Math.random() * 100),
          db.write(metric, start.plusSeconds(86400).getMillis, Math.random() * 100),
          db.write(metric, start.plusSeconds(86401).getMillis, v)
      )), Duration.Inf)

      val result = Await.result(db.read(Seq(metric), start.plusSeconds(86399), start.plusSeconds(86401)), Duration(15, SECONDS))
//      println(result)
      result.keys.size === 1
      result(metric).length === 3
      result(metric).lastOption.flatMap(_.value) === Some(v)
    }

    "read ranges outside bounds" in {
      val v = Math.random() * 100
      Await.ready(db.write(metric, start, v), Duration.Inf)

      val result = Await.result(db.read(Seq(metric), start.minusMinutes(1), start.plusSeconds(10)), Duration(15, SECONDS))
//      println(result)
      result.keys.size === 1
      result(metric).length === 71
      result(metric).drop(60).head.value === Some(v)
    }

    "read non-existent metrics" in {
      val result = Await.result(db.read(Seq(metric), start, start.plusHours(24).minusSeconds(1)), Duration(15, SECONDS))
      result.keys.size === 0
    }

    "read/write multiple metrics" in {
      val v1 = Math.random() * 100
      val v2 = Math.random() * 100

      Await.ready(Future.sequence(List(
        db.write("impressions", start, v1),
        db.write("impressions", start.plusSeconds(1), Math.random() * 100),
        db.write("impressions", start.plusSeconds(2), Math.random() * 100),
        db.write("conversions", start, v2),
        db.write("conversions", start.plusSeconds(1), Math.random() * 100)
      )), Duration.Inf)

      val result = Await.result(db.read(List("impressions", "conversions"), start, start.plusSeconds(4)), Duration(15, SECONDS))
//      println(result)
      result.keys.size === 2
      result("impressions").length === 5
      result("impressions").headOption.flatMap(_.value) === Some(v1)
      result("conversions").length === 5
      result("conversions").headOption.flatMap(_.value) === Some(v2)
    }

    "read wildcards" in {
      val v1 = Math.random() * 100
      val v2 = Math.random() * 100

      Await.ready(Future.sequence(List(
        db.write("stats.impressions", start, v1),
        db.write("stats.impressions", start.plusSeconds(1), Math.random() * 100),
        db.write("stats.impressions", start.plusSeconds(2), Math.random() * 100),
        db.write("stats.conversions", start, v2),
        db.write("stats.conversions", start.plusSeconds(1), Math.random() * 100)
      )), Duration.Inf)

      val result = Await.result(db.read(List("stats.*"), start, start.plusSeconds(4)), Duration(15, SECONDS))
//      println(result)
      result.keys.size === 2
      result("stats.impressions").length === 5
      result("stats.impressions").headOption.flatMap(_.value) === Some(v1)
      result("stats.conversions").length === 5
      result("stats.conversions").headOption.flatMap(_.value) === Some(v2)
    }

    "read/write a day's worth of data" in {
      val begin = System.currentTimeMillis()

      Await.ready(Future.sequence(0 until 86400 map { i =>
        db.write(metric, start.plusSeconds(i), Math.random() * 100)
      }), Duration.Inf)

      println(s"write time ${System.currentTimeMillis() - begin}")

      val readStart = System.currentTimeMillis()
      val result = Await.result(db.read(Seq(metric), start, start.plusDays(1).minusSeconds(1)), Duration.Inf)
      println(s"read time = ${System.currentTimeMillis() - readStart}")
      result(metric).length === 86400
    }
  }
}
