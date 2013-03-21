name := "tsdb"

version := "0.1.0"

scalaVersion := "2.10.1"

libraryDependencies ++= Seq(
  "com.typesafe.akka"  %% "akka-actor"    % "2.1.2",
  "com.typesafe.akka"  %% "akka-agent"    % "2.1.2",
  "joda-time"           % "joda-time"     % "2.2",
  "net.sf.ehcache"      % "ehcache"       % "2.7.0"
)
