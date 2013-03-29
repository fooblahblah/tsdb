package voltdb.procedures

import org.voltdb._

class Insert extends VoltProcedure {

  val checkStmt  = new SQLStmt("""SELECT metric FROM timeseries WHERE metric = ? AND time = ?;""")

  val insertStmt = new SQLStmt("""INSERT INTO TIMESERIES VALUES (?, ?, ?);""")

  val updateStmt = new SQLStmt("""UPDATE TIMESERIES SET value = value + ? WHERE metric=? and time=?;""")

  def run(metric: String, timestamp: Long, value: Double): Array[VoltTable] = {
    // Check whether the pair exists
    voltQueueSQL(checkStmt, metric.asInstanceOf[Object], timestamp.asInstanceOf[Object])

    // Insert new or update existing key depending on result
    if (voltExecuteSQL()(0).getRowCount() == 0)
      voltQueueSQL(insertStmt, metric.asInstanceOf[Object], timestamp.asInstanceOf[Object], value.asInstanceOf[Object])
    else
      voltQueueSQL(updateStmt, value.asInstanceOf[Object], metric.asInstanceOf[Object], timestamp.asInstanceOf[Object])

    voltExecuteSQL()
  }
}