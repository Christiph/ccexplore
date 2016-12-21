name := "NamedEntityPredict"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies += "org.apache.spark" % "spark-core_2.10" % "1.5.2"

libraryDependencies += "org.apache.spark" % "spark-mllib_2.10" % "1.5.2"

libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "2.7.1" excludeAll ExclusionRule(organization = "javax.servlet")

resolvers += "Kunyan Repo" at "http://222.73.34.92:8081/nexus/content/groups/public/"

libraryDependencies += "com.kunyan" % "nlpsuit-package" % "0.2.7.4.test"

libraryDependencies += "org.ansj" % "ansj_seg" % "0.9"
    