name := "tsdb"

version := "0.1.0"

scalaVersion := "2.10.1"

libraryDependencies ++= Seq(
  "com.typesafe.akka"  %% "akka-actor"    % "2.1.2",
  "com.typesafe.akka"  %% "akka-agent"    % "2.1.2",
  "javax.transaction"   % "jta"           % "1.1",
  "joda-time"           % "joda-time"     % "1.6.2",
  "net.sf.ehcache"      % "ehcache"       % "2.7.0",
  "org.javolution"      % "javolution"    % "5.3.1",
  "org.scalaz"         %% "scalaz-core"   % "7.0.0-M8"
)

resolvers += "Sonatype SourceForge" at "https://oss.sonatype.org/content/groups/sourceforge/"
