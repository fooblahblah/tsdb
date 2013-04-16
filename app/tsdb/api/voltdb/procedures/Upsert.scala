package tsdb.api.voltdb.procedures

import org.voltdb._

class Upsert extends VoltProcedure {

  val checkStmt  = new SQLStmt("""SELECT value FROM timeseries WHERE metric = ? AND time = ?;""")

  val insertStmt = new SQLStmt("""INSERT INTO TIMESERIES VALUES (?, ?, ?);""")

  val updateStmt = new SQLStmt("""UPDATE TIMESERIES SET value = ? WHERE metric=? and time=?;""")

  def run(metric: String, timestamp: Long, value: Double): Array[VoltTable] = {
    // Check whether the pair exists
    voltQueueSQL(checkStmt, metric.asInstanceOf[Object], timestamp.asInstanceOf[Object])
    val selectResults = voltExecuteSQL()

    // Insert new or update existing key depending on result
    if (selectResults(0).advanceRow() == true) {
      val total = value + selectResults(0).getDouble(0)
      voltQueueSQL(updateStmt, total.asInstanceOf[Object], metric.asInstanceOf[Object], timestamp.asInstanceOf[Object])
    }
    else
      voltQueueSQL(insertStmt, metric.asInstanceOf[Object], timestamp.asInstanceOf[Object], value.asInstanceOf[Object])

    voltExecuteSQL(true)
  }
}