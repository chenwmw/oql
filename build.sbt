val parser = "org.scala-lang.modules" %% "scala-parser-combinators" % "1.0.4"
val scalatest = "org.scalatest" % "scalatest_2.11" % "2.2.4" % "test"
val junit = "junit" % "junit" % "4.12" % "test"

lazy val commonSettings = Seq(
	organization := "com.zuora",
	version := "1.0",
	scalaVersion := "2.11.7"
)

lazy val root = (project in file(".")).
settings(commonSettings: _*).
settings(name := "oql").
settings(libraryDependencies += parser).
settings(libraryDependencies += scalatest).
settings(libraryDependencies += junit)
