package tsdb.api.voltdb.procedures

import org.voltdb._

class Find extends VoltProcedure {
  val findStmt = new SQLStmt(s"""SELECT metric, time, value FROM timeseries WHERE metric LIKE ? AND time >= ? AND time <= ? ORDER BY time ASC;""")

  def run(metric: String, start: Long, end: Long): Array[VoltTable] = {
    voltQueueSQL(findStmt, metric.replaceAll("""\*""", "%").asInstanceOf[Object], start.asInstanceOf[Object], end.asInstanceOf[Object])
    voltExecuteSQL(true)
  }
}

