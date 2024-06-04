ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.12"

lazy val root = (project in file("."))
  .settings(
    name := "job01",
    idePackagePrefix := Some("com.dodat.scala")
  )

val sparkVersion = "3.5.0"

libraryDependencies ++= Seq (
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-core" % sparkVersion
)