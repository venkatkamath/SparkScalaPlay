name := "SparkScalaPlay"

version := "1.0"

scalaVersion := "2.10.6"

// https://mvnrepository.com/artifact/org.apache.spark/spark-core_2.10
libraryDependencies += "org.apache.spark" % "spark-core_2.10" % "1.6.2"
libraryDependencies += "org.apache.spark" % "spark-sql_2.10" % "1.6.2"
// https://mvnrepository.com/artifact/com.datastax.spark/spark-cassandra-connector_2.10
libraryDependencies += "com.datastax.spark" % "spark-cassandra-connector_2.10" % "1.6.2"



