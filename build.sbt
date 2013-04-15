name := "tsdb"

version := "0.1.0"

scalaVersion := "2.10.1"

libraryDependencies ++= Seq(
  "com.google.guava"         %  "guava"                % "14.0.1",
  "com.typesafe.akka"        %% "akka-actor"           % "2.1.2",
  "com.typesafe.akka"        %% "akka-agent"           % "2.1.2",
  "joda-time"                %  "joda-time"            % "1.6.2",
  "junit"                    %  "junit"                % "latest.integration" % "test",
  "org.scalaz"               %% "scalaz-core"          % "7.0.0-M8",
  "org.specs2"               %% "specs2"               % "latest.integration" % "test"
)

resolvers += "Sonatype SourceForge" at "https://oss.sonatype.org/content/groups/sourceforge/"
