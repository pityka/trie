scalaVersion := "2.11.8"

lazy val commonSettings = Seq(
  organization := "trie",
  version := "0.0.1-SNAPSHOT",
  scalaVersion := "2.11.8",
  javacOptions ++= Seq("-Xdoclint:none")

)

lazy val core = project.in(file(".")).
		settings(commonSettings).
		settings(
			name:="trie-core",
      libraryDependencies ++= Seq(
        "org.scalatest" %% "scalatest" % "2.1.5" % "test",
        "org.xerial.snappy" % "snappy-java" % "1.1.2.1")
		)
