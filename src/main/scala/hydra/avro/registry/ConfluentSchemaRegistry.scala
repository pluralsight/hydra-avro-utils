package hydra.avro.registry

import com.typesafe.config.Config
import io.confluent.kafka.schemaregistry.client.{CachedSchemaRegistryClient, MockSchemaRegistryClient, SchemaMetadata, SchemaRegistryClient}

import scala.concurrent.{ExecutionContext, Future}
import scala.collection.JavaConverters._

/**
  * Created by alexsilva on 7/6/17.
  */
trait ConfluentSchemaRegistry extends SchemaRegistryComponent {

  def config: Config

  val registryClient: SchemaRegistryClient = ConfluentSchemaRegistry.fromConfig(config)
  val registryUrl: String = ConfluentSchemaRegistry.registryUrl(config)

  def getAllSubjects()(implicit ec: ExecutionContext): Future[Seq[String]] =
    Future(registryClient.getAllSubjects().asScala.map(_.dropRight(6)).toSeq)

  /**
    * Due to limititations of the client registry API, this will only work for schemas registered within Hydra.
    *
    * @param id
    * @return
    */
  def getById(id: Int)(implicit ec: ExecutionContext): Future[SchemaMetadata] = Future {
    val schema = registryClient.getByID(id)
    val subject = schema.getNamespace + schema.getName + "-value"
    registryClient.getLatestSchemaMetadata(subject)
  }
}

object ConfluentSchemaRegistry {

  import configs.syntax._

  val mockRegistry = new MockSchemaRegistryClient()

  def registryUrl(config: Config) = config.get[String]("schema.registry.url")
    .valueOrThrow(_ => new IllegalArgumentException("A schema registry url is required."))

  def fromConfig(config: Config): SchemaRegistryClient = {
    val url = registryUrl(config)
    val identityMapCapacity = config.get[Int]("schema.registry.map.capacity").valueOrElse(1000)
    if (url == "mock") mockRegistry
    else new CachedSchemaRegistryClient(url, identityMapCapacity)
  }
}


// A wrapper class for Java clients
class ConfluentSchemaRegistryWrapper(val config: Config) extends ConfluentSchemaRegistry
