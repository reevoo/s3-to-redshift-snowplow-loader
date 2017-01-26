name := "snowplow_historic_event_uploader"

version := "1.0"

scalaVersion in ThisBuild := "2.11.7"

resolvers += "Redshift" at "http://redshift-maven-repository.s3-website-us-east-1.amazonaws.com/release"

// additional libraries
libraryDependencies ++= Seq(
  "com.amazon.redshift" % "redshift-jdbc41" % "1.2.1.1001",
  "com.github.nscala-time" %% "nscala-time" % "2.14.0",
  "com.frugalmechanic" %% "scala-optparse" % "1.1.2",
  "com.amazonaws" % "aws-java-sdk" % "1.11.66",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.5.0",
  "ch.qos.logback" % "logback-classic" % "1.1.7"
)
