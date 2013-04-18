package tsdb.api

import anorm._
import Implicits._
import java.io.File
import java.util.Date
import org.joda.time.DateTime
import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner._
import org.specs2.time.Duration
import java.util.concurrent.TimeUnit._
import org.joda.time.DateMidnight
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import scala.concurrent._
import scala.concurrent.duration.Duration
import scala.concurrent.ExecutionContext.Implicits.global
import tsdb.api.Implicits._
import org.specs2.specification.BeforeExample
import play.api.Play.current
import play.api.test._
import play.api.test.Helpers._
import play.api.db.DB
import org.specs2.execute.AsResult
import org.specs2.execute.Result
import org.specs2.specification.AroundExample


@RunWith(classOf[JUnitRunner])
class TSDBSpec extends Specification with AroundExample {
  sequential

  def application = FakeApplication(additionalConfiguration = Map(
    "ehcacheplugin"      -> "disabled",
    "logger.application" -> "ERROR",
    "db.default.url"     -> "jdbc:com.nuodb://localhost/tsdb?schema=test"))

  val db     = TSDB()
  val metric = "stats_counts.site.web_traffic.impression"
  val start  = new DateTime(new DateMidnight())

  def around[R : AsResult](r: => R): Result = {
    running(application) {
      db.truncateTimeseries()
      AsResult(r)
    }
  }

  def randomValue = (Math.floor (Math.random() * 100 * 100)) / 100

  "TSDB" should {
    "read first 5 data points" in {
      val v = randomValue
      Await.ready(Future.sequence(List(
        db.write(metric, start, randomValue),
        db.write(metric, start.plusSeconds(1), randomValue),
        db.write(metric, start.plusSeconds(2), randomValue),
        db.write(metric, start.plusSeconds(3), randomValue),
        db.write(metric, start.plusSeconds(4), v)
      )), Duration.Inf)

      val result = Await.result(db.read(List(metric), start, start.plusSeconds(4)), Duration(15, SECONDS))
//      println(result)
      result.keys.size == 1
      result(metric).length === 5
      result(metric).lastOption.flatMap(_.value) === Some(v)
    }

    "read gappy data" in {
      val v = randomValue
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
      val v = randomValue
      Await.ready(Future.sequence(List(
          db.write(metric, start.plusSeconds(86399).getMillis, randomValue),
          db.write(metric, start.plusSeconds(86400).getMillis, randomValue),
          db.write(metric, start.plusSeconds(86401).getMillis, v)
      )), Duration.Inf)

      val result = Await.result(db.read(Seq(metric), start.plusSeconds(86399), start.plusSeconds(86401)), Duration(15, SECONDS))
//      println(result)
      result.keys.size === 1
      result(metric).length === 3
      result(metric).lastOption.flatMap(_.value) === Some(v)
    }

    "read ranges outside bounds" in {
      val v = randomValue
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
      val v1 = randomValue
      val v2 = randomValue

      Await.ready(Future.sequence(List(
        db.write("impressions", start, v1),
        db.write("impressions", start.plusSeconds(1), randomValue),
        db.write("impressions", start.plusSeconds(2), randomValue),
        db.write("conversions", start, v2),
        db.write("conversions", start.plusSeconds(1), randomValue)
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
      val v1 = randomValue
      val v2 = randomValue

      Await.ready(Future.sequence(List(
        db.write("stats.impressions", start, v1),
        db.write("stats.impressions", start.plusSeconds(1), randomValue),
        db.write("stats.impressions", start.plusSeconds(2), randomValue),
        db.write("stats.conversions", start, v2),
        db.write("stats.conversions", start.plusSeconds(1), randomValue)
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
        db.write(metric, start.plusSeconds(i), randomValue)
      }), Duration.Inf)

      println(s"write time ${System.currentTimeMillis() - begin}")

      val readStart = System.currentTimeMillis()
      val result = Await.result(db.read(Seq(metric), start, start.plusDays(1).minusSeconds(1)), Duration.Inf)
      println(s"read time = ${System.currentTimeMillis() - readStart}")
      result(metric).length === 86400
    }
  }
}
