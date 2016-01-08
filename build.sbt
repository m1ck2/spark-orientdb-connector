lazy val commonSettings = Seq(
  organization := "com.metreta",
  version := "v1.4-2.10",
  scalaVersion := "2.10.4",
  fork:= true,
  libraryDependencies ++= Seq(
    "com.orientechnologies" % "orientdb-core" % "2.1.9",
    "com.orientechnologies" % "orientdb-client" % "2.1.9",
    "com.orientechnologies" % "orientdb-jdbc" % "2.1.9",
    "com.orientechnologies" % "orientdb-graphdb" % "2.1.9",
    "com.orientechnologies" % "orientdb-distributed" % "2.1.9",
    "org.scalatest" % "scalatest_2.10" % "2.2.4",
    "org.apache.spark" % "spark-core_2.10" % "1.4.1",
    "org.apache.spark" % "spark-graphx_2.10" % "1.4.1",
    "com.tinkerpop.blueprints" % "blueprints-core" % "2.6.0"
    ),
    externalResolvers := Seq(DefaultMavenRepository),
    parallelExecution in Test := false
)

lazy val connector = (project in file("./spark-orientdb-connector")).
  settings(commonSettings: _*).
  settings(
    name := "spark-orientdb-connector"
    )

lazy val demos = (project in file("./spark-orientdb-connector-demos")).
  settings(commonSettings: _*).
  settings(
    name := "spark-orientdb-connector-demos"
    ).
   dependsOn(connector)