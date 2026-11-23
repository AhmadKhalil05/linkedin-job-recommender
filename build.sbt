name := "linkedin-srs"

organization := "com.linkedin.recommender"

version := "1.0-SNAPSHOT"

scalaVersion := "2.12.18"

val sparkVersion = "3.3.0"
val kafkaVersion = "3.4.0"
val mongoVersion = "5.5.0"
val orgJsonVersion = "20240303"

javacOptions ++= Seq("-source", "11", "-target", "11")

scalacOptions ++= Seq(
  "-deprecation",
  "-encoding", "UTF-8",
  "-feature",
  "-unchecked"
)

libraryDependencies ++= Seq(
  // Apache Spark
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion,
  "org.apache.spark" %% "spark-token-provider-kafka-0-10" % sparkVersion,

  // Apache Kafka
  "org.apache.kafka" % "kafka-clients" % kafkaVersion,

  // MongoDB Scala Driver
  "org.mongodb.scala" %% "mongo-scala-driver" % mongoVersion,

  // org.json
  "org.json" % "json" % orgJsonVersion,

  // Scala Reflect & Library
  "org.scala-lang" % "scala-reflect" % scalaVersion.value,
  "org.scala-lang" % "scala-library" % scalaVersion.value
)

Compile / mainClass := Some("com.linkedin.recommender.LinkedInRecommenderApp")
