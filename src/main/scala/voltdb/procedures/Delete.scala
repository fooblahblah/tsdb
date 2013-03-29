package voltdb.procedures

import org.voltdb._

class Delete extends VoltProcedure {

  val deleteStmt  = new SQLStmt("""DELETE FROM timeseries;""")

  def run(): Array[VoltTable] = {
    // Check whether the pair exists
    voltQueueSQL(deleteStmt)
    voltExecuteSQL(true)
  }
}