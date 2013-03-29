import sbt._
import Keys._

object TSDBBuild extends Build {

  val compileVoltdb = TaskKey[Int]("compile-voltdb")
  val runVoltdb     = TaskKey[Int]("run-voltdb")

  lazy val root = Project(id = "tsdb", base = file(".")).settings(
    compileVoltdb <<= classDirectory in Compile map { (cd) => """voltdb compile --classpath="%s" -o timeseries.jar timeseries.sql""".format(cd) ! } dependsOn (compile in Compile),
    runVoltdb <<= compileVoltdb map { c => """voltdb create host localhost catalog timeseries.jar deployment deployment.xml""" ! }
  )
}
