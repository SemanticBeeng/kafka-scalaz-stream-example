name := """kafka-scalaz-stream-example"""

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.6" % "test"
libraryDependencies += "org.scalaz" %% "scalaz-core" % "7.2.2"
libraryDependencies += "org.scalaz" %% "scalaz-effect" % "7.2.2"
libraryDependencies += "org.scalaz" %% "scalaz-concurrent" % "7.2.2"
libraryDependencies += "org.scalaz.stream" %% "scalaz-stream" % "0.8"
libraryDependencies += "org.apache.kafka" % "kafka-clients" % "0.9.0.0"
libraryDependencies += "com.google.guava" % "guava" % "19.0"
libraryDependencies += "com.fasterxml.jackson.core" % "jackson-databind" % "2.7.3"
libraryDependencies += "org.hdrhistogram" % "HdrHistogram" % "2.1.8"

mainClass in assembly := Some("com.example.Run")

assemblyJarName in assembly := "output.jar"

