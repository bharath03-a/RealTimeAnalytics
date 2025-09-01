import sbt._
import Keys._
import com.here.bom.Bom
import com.typesafe.sbt.packager.docker._
import scala.sys.process._
import complete.DefaultParsers._


// see https://github.com/spotify/scio/blob/v0.14.9/build.sbt
val scioVersion = "0.14.9"
val beamVersion = "2.60.0"
val circeVersion = "0.14.9"
val guavaVersion = "33.1.0-jre"
val jacksonVersion = "2.15.4"
val magnolifyVersion = "0.7.4"
val nettyVersion = "4.1.100.Final"
val slf4jVersion = "1.7.30"
val gcpLibrariesVersion = "26.45.0"
val flinkVersion = "1.18.0"
val sparkVersion = "3.5.0"


lazy val gcpBom = Bom("com.google.cloud" % "libraries-bom" % gcpLibrariesVersion)
lazy val beamBom = Bom("org.apache.beam" % "beam-sdks-java-bom" % beamVersion)
lazy val guavaBom = Bom("com.google.guava" % "guava-bom" % guavaVersion)
lazy val jacksonBom = Bom("com.fasterxml.jackson" % "jackson-bom" % jacksonVersion)
lazy val magnolifyBom = Bom("com.spotify" % "magnolify-bom" % magnolifyVersion)
lazy val nettyBom = Bom("io.netty" % "netty-bom" % nettyVersion)
lazy val scioBom = Bom("com.spotify" % "scio-bom" % scioVersion)

lazy val gcpProject = settingKey[String]("GCP Project")
lazy val gcpRegion = settingKey[String]("GCP region")
lazy val gcpDataflowFlexBucket = settingKey[String]("GCS bucket for the flext template")
lazy val gcpDataflowFlexTemplateBuild = inputKey[Unit]("create dataflow flex-template")
lazy val gcpDataflowFlexTemplateRun = inputKey[Unit]("run dataflow flex-template")


val bomSettings = Def.settings(
  gcpBom,
  beamBom,
  guavaBom,
  jacksonBom,
  magnolifyBom,
  nettyBom,
  dependencyOverrides ++=
    gcpBom.key.value.bomDependencies ++
    beamBom.key.value.bomDependencies ++
      guavaBom.key.value.bomDependencies ++
      jacksonBom.key.value.bomDependencies ++
      magnolifyBom.key.value.bomDependencies ++
      nettyBom.key.value.bomDependencies,

  dependencyOverrides += "com.github.luben" % "zstd-jni" % "1.5.6-3"

)

lazy val commonSettings = bomSettings ++ Def.settings(
  organization := "dataengineering",
  // Semantic versioning http://semver.org/
  version := "0.1.0-SNAPSHOT",
  scalaVersion := "2.13.15",
  scalacOptions ++= Seq(
    "-release", "8",
    "-deprecation",
    "-feature",
    "-unchecked",
    "-Ymacro-annotations"
  ),
  javacOptions ++= Seq("--release", "8"),
  // add extra resolved and remove exclude if you need kafka
  // resolvers += "confluent" at "https://packages.confluent.io/maven/",
  excludeDependencies += "org.apache.beam" % "beam-sdks-java-io-kafka"
)

lazy val root: Project = project
  .in(file("."))
  .enablePlugins(JavaAppPackaging, DockerPlugin)
  .settings(commonSettings)
  .settings(
    name := "realtimereelanalytics",
    description := "RealTimeReelAnalytics",
    publish / skip := true,
    fork := true,
    run / outputStrategy := Some(OutputStrategy.StdoutOutput),
    libraryDependencies ++= Seq(
      "com.spotify" %% "scio-core" % scioVersion,
      "io.circe" %% "circe-core" % circeVersion,
      "io.circe" %% "circe-generic" % circeVersion,
      "io.circe" %% "circe-parser" % circeVersion,
      "org.slf4j" % "slf4j-api" % slf4jVersion,
      "com.spotify" %% "scio-google-cloud-platform" % scioVersion,
      "org.apache.beam" % "beam-runners-google-cloud-dataflow-java" % beamVersion  % Runtime,
      "org.apache.beam" % "beam-runners-flink-1.16" % beamVersion % Runtime,
      "org.apache.flink" % "flink-clients" % flinkVersion % Runtime,
      "org.apache.flink" % "flink-streaming-java" % flinkVersion % Runtime,
      "org.apache.beam" % "beam-runners-spark-3" % beamVersion % Runtime,
      "org.apache.spark" %% "spark-core" % sparkVersion % Runtime,
      "org.apache.spark" %% "spark-streaming" % sparkVersion % Runtime,
      "org.apache.beam" % "beam-runners-direct-java" % beamVersion % Test,
      "com.spotify" %% "scio-test" % scioVersion % Test,
      "org.slf4j" % "slf4j-simple" % slf4jVersion % Test,
      "org.slf4j" % "slf4j-simple" % "2.0.9"
    ),
    Docker / packageName := s"gcr.io/${gcpProject.value}/dataflow/templates/${name.value}",
    Docker / daemonUserUid := None,
    Docker / daemonUser := "root",
    dockerPermissionStrategy := DockerPermissionStrategy.None,
    dockerBaseImage := "gcr.io/dataflow-templates-base/java11-template-launcher-base:latest",
    dockerCommands := {
      // keep default from base image
      val filteredCommands = dockerCommands.value.filterNot {
        case Cmd("USER", _*) => true
        case Cmd("WORKDIR", _*) => true
        case ExecCmd("ENTRYPOINT", _*) => true
        case ExecCmd("CMD", _*) => true
        case _ => false
      }
      // add required ENV commands
      val envCommands = Seq(
        Cmd(
          "ENV",
          "FLEX_TEMPLATE_JAVA_MAIN_CLASS",
          (Compile / mainClass).value.get
        ),
        Cmd(
          "ENV",
          "FLEX_TEMPLATE_JAVA_CLASSPATH",
          (Docker / defaultLinuxInstallLocation).value + "/lib/*"
        )
      )
      filteredCommands ++ envCommands
    },
    gcpProject := "", // TODO
    gcpRegion := "", // TODO
    gcpDataflowFlexBucket := "", // TODO
    gcpDataflowFlexTemplateBuild := {
      (Docker / publish).value
      s"""gcloud dataflow flex-template build gs://${gcpDataflowFlexBucket.value}/dataflow/templates/${name.value}.json
         |--image ${dockerAlias.value}
         |--sdk-language JAVA
         |--metadata-file metadata.json""".stripMargin !
    },
    gcpDataflowFlexTemplateRun := {
      val parameters = spaceDelimited("<arg>").parsed
      s"""gcloud dataflow flex-template run ${name.value}
         |--project=${gcpProject.value}
         |--region=${gcpRegion.value}
         |--temp-location=gs://${gcpDataflowFlexBucket.value}/dataflow/temp
         |--staging-location=gs://${gcpDataflowFlexBucket.value}/dataflow/staging
         |--template-file-gcs-location=gs://${gcpDataflowFlexBucket.value}/dataflow/templates/${name.value}.json
         |--parameters ${parameters.mkString(",")}""".stripMargin !
    }
  )

lazy val repl: Project = project
  .in(file(".repl"))
  .settings(commonSettings)
  .settings(
    name := "repl",
    description := "Scio REPL for RealTimeReelAnalytics",
    libraryDependencies ++= Seq(
      "com.spotify" %% "scio-repl" % scioVersion
    ),
    Compile / mainClass := Some("com.spotify.scio.repl.ScioShell"),
    publish / skip := true,
    fork := false,
  )
  .dependsOn(root)
