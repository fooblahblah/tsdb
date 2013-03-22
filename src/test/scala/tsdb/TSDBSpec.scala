package tsdb

import scala.concurrent.ExecutionContext.Implicits.global
import java.util.Date
import org.joda.time._
import org.specs2.mutable.Specification
import org.junit.runner._
import org.specs2.runner._

@RunWith(classOf[JUnitRunner])
class TSDBSpec extends Specification {
   val db        = new TSDB("/tmp/tsdb.h5")
   val path      = "stats_counts/site/web_traffic/impression"
   val startTime = System.currentTimeMillis()

  "TSDB" should {
    step {
      println("writing test data")

      0 until TSDB.SECONDS_PER_DAY foreach { i =>
        val t = startTime + (i * 1000)
        db.write(path, t, Math.random() * 100)
      }

      println(s"write time: ${System.currentTimeMillis() - startTime}")
    }

    "Read basic range" in {
      val start = new DateTime(startTime)

      val startTime2 = System.currentTimeMillis()

      val results = db.read(path, start.toDate.getTime, start.plusHours(1).toDate.getTime)
      println(s"${results.length}, ${results.headOption.map(e => new Date(e.timestamp))}, ${results.lastOption.map(e => new Date(e.timestamp))}")
      println(s"read time: ${System.currentTimeMillis() - startTime2}")
      results.length == 3601
    }

    step {
      db.stop map { _ =>
        println("done!")
      }
    }
  }
}
