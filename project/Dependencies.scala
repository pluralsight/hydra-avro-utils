import sbt.{ExclusionRule, _}


object Dependencies {

  val scalaTestVersion = "3.0.1"
  val slf4jVersion = "1.7.21"
  val log4jVersion = "2.7"
  val kxbmapConfigVersion = "0.4.4"
  val typesafeConfigVersion = "1.3.0"
  val avroVersion = "1.8.1"
  val jodaTimeVersion = "2.9.3"
  val jodaConvertVersion = "1.8.1"
  val confluentVersion = "3.2.1"
  val kafkaVersion = "0.10.2.0"
  val scalazVersion = "7.2.9"
  val scalaCacheVersion = "0.9.3"
  val jacksonVersion = "2.8.4"
  val springVersion = "4.2.2.RELEASE"

  object Compile {

    val scalaConfigs = "com.github.kxbmap" %% "configs" % kxbmapConfigVersion

    val typesafeConfig = "com.typesafe" % "config" % typesafeConfigVersion

    val scalaz = "org.scalaz" %% "scalaz-core" % scalazVersion

    val kafka = Seq(
      "org.apache.kafka" %% "kafka" % kafkaVersion,
      "org.apache.kafka" % "kafka-clients" % kafkaVersion,
      "net.manub" %% "scalatest-embedded-kafka" % "0.12.0" % "test")

    val confluent = Seq("io.confluent" % "kafka-schema-registry-client" % confluentVersion,
      "io.confluent" % "kafka-avro-serializer" % confluentVersion).map(_.excludeAll(
      ExclusionRule(organization = "org.codehaus.jackson"),
      ExclusionRule(organization = "com.fasterxml.jackson.core")))

    val log4J = Seq(
      "org.apache.logging.log4j" % "log4j-slf4j-impl" % log4jVersion,
      "org.apache.logging.log4j" % "log4j-core" % log4jVersion,
      "org.apache.logging.log4j" % "log4j-api" % log4jVersion,
      "org.apache.logging.log4j" % "log4j-1.2-api" % log4jVersion)

    val guavacache = "com.github.cb372" %% "scalacache-guava" % scalaCacheVersion

    val avro = "org.apache.avro" % "avro" % avroVersion

    val springCore = "org.springframework" % "spring-core" % springVersion

    val joda = Seq(
      "joda-time" % "joda-time" % jodaTimeVersion,
      "org.joda" % "joda-convert" % jodaConvertVersion)

    val jackson = Seq(
      "com.fasterxml.jackson.core" % "jackson-core" % jacksonVersion,
      "com.fasterxml.jackson.core" % "jackson-databind" % jacksonVersion
    )
  }

  object Test {
    val scalaTest = "org.scalatest" %% "scalatest" % scalaTestVersion % "test"
    val junit = "junit" % "junit" % "4.12" % "test"
  }


  import Compile._
  import Test._

  val testDeps = Seq(scalaTest, junit)

  val baseDeps = log4J ++ Seq(scalaz, scalaConfigs, avro, springCore) ++ jackson ++ testDeps

  val coreDeps = baseDeps ++ Seq(guavacache) ++ confluent ++ kafka

  val overrides = Set(log4J, typesafeConfig, joda)
}

