ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "3.1.3"
//val scalaVersion = "3.1.3"
lazy val root = (project in file("."))
  .settings(
    name := "LogFileProcessor",
    organization := "edu.uic.anair38",
    version := "0.1.0-SNAPSHOT",
  )


// https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-common
libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "3.3.4"
// https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-mapreduce-client-core
libraryDependencies += "org.apache.hadoop" % "hadoop-mapreduce-client-core" % "3.3.4"
libraryDependencies += "org.apache.hadoop" % "hadoop-mapreduce-client-jobclient" % "3.3.4"
//libraryDependencies += "com.typesafe.scala-logging" %% "scala-logging" % "3.7.2"
//libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.2.3"
//libraryDependencies ++= Seq("org.slf4j" % "slf4j-api" % "1.7.5",
//  "org.slf4j" % "slf4j-simple" % "1.7.5")
libraryDependencies ++= Seq(
  "com.typesafe" % "config" % "1.3.3",
  "ch.qos.logback" % "logback-classic" % "1.2.3",
  "ch.qos.logback" % "logback-core" % "1.2.3",
  "org.slf4j" % "slf4j-api" % "1.7.5",
)
libraryDependencies += "org.scalatestplus" %% "mockito-4-6" % "3.2.14.0" % "test"
libraryDependencies += "org.scalactic" %% "scalactic" % "3.2.14"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.14" % "test"


assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}