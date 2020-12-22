name := "clear-score-reports"

version := "0.1"

scalaVersion := "2.12.12"


libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.0.1",
  "org.apache.spark" %% "spark-sql" % "3.0.1",
  "org.rogach" %% "scallop" % "4.0.1",

  "org.scalatest" %% "scalatest" % "3.0.8" % "test"
)
