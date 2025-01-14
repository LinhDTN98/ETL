
ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.17"

lazy val root = (project in file("."))
  .settings(
    name := "projectETL"
  )

val spark_version = "3.4.0"

// https://mvnrepository.com/artifact/org.apache.spark/

libraryDependencies +="org.apache.spark" %% "spark-core" % spark_version

libraryDependencies +="org.apache.spark" %% "spark-sql" % spark_version % "provided"

libraryDependencies +="org.apache.spark" %% "spark-hive" % spark_version % "provided"

libraryDependencies += "org.postgresql" % "postgresql" % "42.7.3"

libraryDependencies += "io.delta" %% "delta-core" % "2.4.0"
