package tsdb

import scala.concurrent.ExecutionContext.Implicits.global

object TSDBTester extends App {
  val db = new TSDB("/tmp/tsdb.h5")

  val startTime = System.currentTimeMillis()

  1 to 86400 foreach { i =>
    val t = startTime + (i * 1000)
    db.write("stats_counts/cupcake/web_traffic/impression", t, Math.random() * 100)
    db.write("stats_counts/cupcake/web_traffic/conversion", t, Math.random() * 10)
  }

  println(s"elapsed: ${System.currentTimeMillis() - startTime}")

  println("done!")

  db.stop
}
