name := "demo"
version := "0.1.0"
scalaVersion := "2.10.5"
crossScalaVersions := Seq("2.11.5", "2.10.5")
organization := "com.azavea"
licenses := Seq("Apache-2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0.html"))
scalacOptions ++= Seq(
  "-deprecation",
  "-unchecked",
  "-Yinline-warnings",
  "-language:implicitConversions",
  "-language:reflectiveCalls",
  "-language:higherKinds",
  "-language:postfixOps",
  "-language:existentials",
  "-feature")
publishMavenStyle := true
publishArtifact in Test := false
pomIncludeRepository := { _ => false }

shellPrompt := { s => Project.extract(s).currentProject.id + " > " }

// We need to bump up the memory for some of the examples working with the landsat image.
javaOptions += "-Xmx4G"

fork in run := true

connectInput in run := true

libraryDependencies ++= Seq(
  "com.azavea.geotrellis" %% "geotrellis-spark" % "0.10.1",
  "org.apache.spark" %% "spark-core" % "1.5.2",
  Dependencies.sprayRouting,
  Dependencies.sprayCan,
  "org.scalatest"       %%  "scalatest"      % "2.2.0" % "test"
)

Revolver.settings

assemblyMergeStrategy in assembly := {
  case "reference.conf" => MergeStrategy.concat
  case "application.conf" => MergeStrategy.concat
  case "META-INF/MANIFEST.MF" => MergeStrategy.discard
  case "META-INF\\MANIFEST.MF" => MergeStrategy.discard
  case "META-INF/ECLIPSEF.RSA" => MergeStrategy.discard
  case "META-INF/ECLIPSEF.SF" => MergeStrategy.discard
  case _ => MergeStrategy.first
}

net.virtualvoid.sbt.graph.Plugin.graphSettings
