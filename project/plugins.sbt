addSbtPlugin("io.spray"         % "sbt-revolver"    % "0.9.1")
addSbtPlugin("com.typesafe.sbt" % "sbt-multi-jvm"   % "0.4.0")
addSbtPlugin("org.scalameta"    % "sbt-scalafmt"    % "2.3.0")
addSbtPlugin("ch.epfl.scala"    % "sbt-scalafix"    % "0.9.11")
addSbtPlugin("org.scoverage"    % "sbt-scoverage"   % "1.6.1")
addSbtPlugin("org.wartremover"  % "sbt-wartremover" % "2.4.3")
addSbtPlugin("com.jsuereth"     % "sbt-pgp"         % "2.0.1")
addSbtPlugin("com.geirsson"     % "sbt-ci-release"  % "1.5.0")
addSbtPlugin("com.thesamet"     % "sbt-protoc"      % "0.99.27")

libraryDependencies ++= Seq(
  "com.thesamet.scalapb" %% "compilerplugin" % "0.9.4"
)
