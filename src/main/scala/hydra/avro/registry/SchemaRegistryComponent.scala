package hydra.avro.registry

import com.typesafe.config.Config
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient

/**
  * Created by alexsilva on 2/21/17.
  */
trait SchemaRegistryComponent {

  def config:Config

  def registryClient: SchemaRegistryClient
}
