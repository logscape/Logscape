import ScalateKeys._

name := "play"

version := "1.0.0"

scalaVersion := "2.10"

scalacOptions ++= Seq("-deprecation")

seq(webSettings :_*)

unmanagedJars in Compile <++= baseDirectory map { base =>
  val baseDirectories = (base / "lib")  +++
    (base / ".." / "lib" / "lib") +++
    (base / ".." / "common" / "dist") +++
    (base / ".." / "transport" / "dist") +++
    (base / ".." / "replicator" / "dist") +++
    (base / ".." / "vs-log" / "dist") +++
    (base / ".." / "vs-admin" / "dist") +++
    (base / ".." / "vs-orm" / "dist") +++
    (base / ".." / "dashboardServer" / "dist") +++
    (base / ".." / "vspace" / "dist") +++
    (base / ".." / "vso" / "dist")
  val customJars = (baseDirectories ** "*.jar")
  customJars.classpath
}

libraryDependencies += "org.scalatest" %% "scalatest" % "1.8" % "test"

libraryDependencies += "junit" % "junit" % "4.10" % "test"

libraryDependencies += "org.fusesource.scalate" % "scalate-core" % "2.11-1.7.0" % "compile"

libraryDependencies += "org.fusesource.scalate" % "scalate-util" % "2.11-1.7.0" % "compile"

resolvers += "repo.codahale.com" at "http://repo.codahale.com"

libraryDependencies += "com.codahale" % "jerkson_2.9.1" % "0.5.0" % "compile"

libraryDependencies ++= Seq(
  "org.eclipse.jetty" % "jetty-webapp" % "8.1.9.v20130131" % "container"
    )

libraryDependencies ++= Seq(
  "org.eclipse.jetty.orbit" % "javax.servlet" % "3.0.0.v201112011016" % "container"         artifacts (Artifact("javax.servlet", "jar", "jar")
    )
  )



seq(scalateSettings:_*)

scalateTemplateConfig in Compile <<= (sourceDirectory in Compile){ base =>
  Seq(
    TemplateConfig(
      base / "webapp" / "WEB-INF",
      Seq(
      "import _root_.scala.collection.JavaConversions._",
      "import _root_.org.fusesource.scalate.support.TemplateConversions._",
      "import _root_.org.fusesource.scalate.util.Measurements._"
      ),
      Seq(
      ),
      Some("com.logscape")
    ))}
