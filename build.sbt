name := "rascql"

version := "0.1"

scalaVersion := "2.11.6"

scalacOptions ++= (
  Opts.compile.encoding("UTF8")
    :+ Opts.compile.deprecation
    :+ Opts.compile.unchecked
    :+ "-feature"
)

resolvers += "Typesafe Releases" at "http://repo.typesafe.com/typesafe/releases"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-actor" % "2.3.10",
  "com.typesafe.akka" %% "akka-stream-experimental" % "1.0-RC1",
  "org.scalatest" %% "scalatest" % "2.2.1" % "test"
)

