package voltdb.procedures

import org.voltdb._

class Find extends VoltProcedure {

  val findStmt  = new SQLStmt("""SELECT time, value FROM timeseries WHERE metric = ? AND time >= ? AND time <= ?;""")

  def run(metric: String, start: Long, end: Long): Array[VoltTable] = {
    // Check whether the pair exists
    voltQueueSQL(findStmt, metric.asInstanceOf[Object], start.asInstanceOf[Object], end.asInstanceOf[Object])
    voltExecuteSQL(true)
  }
}