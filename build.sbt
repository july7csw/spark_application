name := "spark-app"
version := "0.1"
scalaVersion := "2.13.10"
val sparkVersion = "3.3.3"

libraryDependencies ++= Seq(
  // Spark dependencies
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,

  // Hive dependency
  "org.apache.spark" %% "spark-hive" % sparkVersion
)