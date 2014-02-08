name := "KMeans Spark Java"

version := "1.0"

scalaVersion := "2.10.3"

seq(com.github.retronym.SbtOneJar.oneJarSettings: _*)

libraryDependencies += "org.apache.spark" %% "spark-core" % "0.9.0-incubating"

libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "1.0.3"

resolvers += "Akka Repository" at "http://repo.akka.io/releases/"



mainClass in (Compile, run) := Some("com.drelu.KMeans")
