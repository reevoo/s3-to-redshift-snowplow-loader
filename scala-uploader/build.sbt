name := "snowplow_historic_event_uploader"

version := "1.0"

scalaVersion in ThisBuild := "2.11.7"

logBuffered in Test := false

resolvers ++= Seq(
  "Redshift" at "http://redshift-maven-repository.s3-website-us-east-1.amazonaws.com/release",
  "Artima Maven Repository" at "http://repo.artima.com/releases"
)

// additional libraries
libraryDependencies ++= Seq(
  "com.amazon.redshift" % "redshift-jdbc41" % "1.2.1.1001",
  "com.github.nscala-time" %% "nscala-time" % "2.14.0",
  "com.frugalmechanic" %% "scala-optparse" % "1.1.2",
  "com.amazonaws" % "aws-java-sdk" % "1.11.66",
  "org.scalactic" %% "scalactic" % "3.0.1",
  "org.scalatest" %% "scalatest" % "3.0.1" % "test"
)
