name := "spark_emr_dev"

version := "1.0"

scalaVersion := "2.11.8"

val sparkVersion = "2.3.0"

resolvers += Resolver.sonatypeRepo("snapshots")

libraryDependencies ++= Seq(
  // scala test 
  "org.scalactic" %% "scalactic" % "3.0.8",
  "org.scalatest" %% "scalatest" % "3.0.8" % "test",
  // config
  "com.typesafe" % "config" % "1.2.1", 
  // spark  
  "org.apache.spark" %% "spark-core" % "2.3.0",
  "org.apache.spark" %% "spark-sql" % "2.3.0",
  "org.apache.spark" %% "spark-mllib" % "2.2.0",
  "com.databricks" %% "spark-csv" % "1.4.0",
  "com.amazonaws" % "aws-java-sdk" % "1.7.4",
  "org.apache.hadoop" % "hadoop-aws" % "2.7.6",
  "org.apache.spark" %% "spark-hive" % "2.4.3"
)

conflictManager := ConflictManager.latestRevision

// scala test : Buffered Output
logBuffered in Test := false

//mainClass := Some("rdd.WordCount")

mainClass := Some("EmrHelloworld.emr_helloworld")

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}