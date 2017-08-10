/*
 * Copyright (C) 2017 Pluralsight, LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package hydra.avro.registry

import com.typesafe.config.ConfigFactory
import io.confluent.kafka.schemaregistry.client.{CachedSchemaRegistryClient, MockSchemaRegistryClient}
import org.apache.avro.Schema
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterAll, FunSpecLike, Matchers}

/**
  * Created by alexsilva on 9/16/16.
  */
class ConfluentSchemaRegistrySpec extends Matchers with FunSpecLike with BeforeAndAfterAll with ScalaFutures {

  private var id = 0

  val schema = new Schema.Parser().parse(
    """
      |{
      |	"type": "record",
      |	"name": "Tester",
      |	"namespace": "hydra",
      |	"fields": [{
      |			"name": "id",
      |			"type": "int"
      |		}
      |	]
      |}""".stripMargin)


  override def beforeAll(): Unit = {
    id = ConfluentSchemaRegistry.mockRegistry.register(schema.getFullName, schema)
  }

  describe("When creating a schema registry client") {
    it("returns a mock") {
      val c = ConfluentSchemaRegistry.forConfig(ConfigFactory.parseString("schema.registry.url=mock"))
      c.registryClient shouldBe a[MockSchemaRegistryClient]
      c.registryUrl shouldBe "mock"
    }

    it("uses a config path") {
      val c = ConfluentSchemaRegistry
        .forConfig("hydra", ConfigFactory.parseString("hydra.schema.registry.url=\"http://localhost:9092\""))
      c.registryUrl shouldBe "http://localhost:9092"
    }

    it("returns all subjects") {
      implicit val ec = scala.concurrent.ExecutionContext.Implicits.global
      val c = ConfluentSchemaRegistry.forConfig(ConfigFactory.parseString("schema.registry.url=mock"))
      whenReady(c.getAllSubjects) { r => r shouldBe Seq(schema.getFullName) }
    }

    it("returns by id") {
      implicit val ec = scala.concurrent.ExecutionContext.Implicits.global
      val c = ConfluentSchemaRegistry.forConfig(ConfigFactory.parseString("schema.registry.url=mock"))
      whenReady(c.getById(id, "")) { r =>
        r.getId shouldBe id
        r.getVersion shouldBe 1
        new Schema.Parser().parse(r.getSchema) shouldBe schema
      }
    }

    it("throws an error if no config key is found") {
      val config = ConfigFactory.empty
      intercept[IllegalArgumentException] {
        ConfluentSchemaRegistry.forConfig("", config)
      }
    }

    it("returns a cached client when using a url") {
      val config = ConfigFactory.parseString("schema.registry.url=\"http://localhost:9092\"")
      ConfluentSchemaRegistry.forConfig("", config).registryClient shouldBe a[CachedSchemaRegistryClient]
      ConfluentSchemaRegistry.registryUrl(config) shouldBe "http://localhost:9092"
    }
  }

}
