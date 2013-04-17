import sbt._
import Keys._
import play._

object TSDBBuild extends Build {

  val appName      = "tsdb"
  val appVersion   = "0.1.0"
  val scalaVersion = "2.10.1"

  val appDependencies = Seq(
    "com.jolbox"               %  "bonecp"               % "0.8.0-rc1",
    "com.typesafe.akka"        %% "akka-actor"           % "2.1.2",
    "com.typesafe.akka"        %% "akka-agent"           % "2.1.2",
    "joda-time"                %  "joda-time"            % "1.6.2",
    "junit"                    %  "junit"                % "latest.integration" % "test",
    "org.scalaz"               %% "scalaz-core"          % "7.0.0-M8",
    "org.specs2"               %% "specs2"               % "latest.integration" % "test",
    "play"                     %% "anorm"                % "2.1.1",
    "play"                     %% "play-jdbc"            % "2.1.1",
    "play"                     %% "play"                 % "2.1.1"
  )

  lazy val root = play.Project(appName, appVersion, appDependencies).settings(
    resolvers ++= Seq("Sonatype SourceForge" at "https://oss.sonatype.org/content/groups/sourceforge/",
                      "Typesafe" at "http://repo.typesafe.com/typesafe/releases"),
    scalacOptions ++= Seq("-language:implicitConversions", "-language:postfixOps"),
    javacOptions ++= Seq("-source", "1.5")
  )
}
