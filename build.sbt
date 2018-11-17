name := "Dslab"

version := "0.1"


scalaVersion := "2.11.8"






libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.3.2" withSources() withJavadoc(),
  "org.apache.spark" %% "spark-mllib" % "2.3.2"  withSources() withJavadoc(),
  
)