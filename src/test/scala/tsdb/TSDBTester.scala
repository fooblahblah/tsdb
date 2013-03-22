package tsdb

import scala.concurrent.ExecutionContext.Implicits.global
import org.joda.time.Minutes

object TSDBTester extends App {
  val db = new TSDB("/tmp/tsdb.h5")

  val startTime = System.currentTimeMillis()

  val path = "stats_counts/cupcake/web_traffic/impression"

  1 to TSDB.SECONDS_PER_DAY foreach { i =>
    val t = startTime + (i * 1000)
    db.write(path, t, Math.random() * 100)
//    db.write("stats_counts/cupcake/web_traffic/conversion", t, Math.random() * 10)
  }

  println(s"write time: ${System.currentTimeMillis() - startTime}")

  val start = startTime + ((TSDB.SECONDS_PER_DAY / 2) * 1000)

  val startTime2 = System.currentTimeMillis()

  println(db.read(path, start, start + (1000 * 60 * 10)))

  println(s"read time: ${System.currentTimeMillis() - startTime2}")

  db.stop map { _ =>
    println("done!")
  }
}
