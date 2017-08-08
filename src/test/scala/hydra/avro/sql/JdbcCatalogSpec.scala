package hydra.avro.sql

import java.util.Properties

import com.typesafe.config.ConfigFactory
import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import org.apache.avro.Schema
import org.scalatest.{BeforeAndAfterAll, FunSpecLike, Matchers}

/**
  * Created by alexsilva on 7/12/17.
  */
class JdbcCatalogSpec extends Matchers with FunSpecLike with BeforeAndAfterAll {

  import scala.collection.JavaConverters._

  val cfg = ConfigFactory.load().getConfig("db-cfg")

  val properties = new Properties

  cfg.entrySet().asScala.foreach(e => properties.setProperty(e.getKey(), cfg.getString(e.getKey())))

  private val hikariConfig = new HikariConfig(properties)

  private val ds = new HikariDataSource(hikariConfig)

  val store = new JdbcCatalog(ds, NoOpSyntax, NoopDialect)

  val schemaStr =
    """
      |{
      |	"type": "record",
      |	"name": "User",
      |	"namespace": "hydra",
      |	"fields": [{
      |			"name": "id",
      |			"type": "int",
      |			"doc": "doc"
      |		},
      |		{
      |			"name": "username",
      |			"type": ["null", "string"]
      |		}
      |	]
      |}""".stripMargin

  val schema = new Schema.Parser().parse(schemaStr)

  override def beforeAll() = {
    store.createOrAlterTable(Table("test_table", schema))
    store.createSchema("test_schema") shouldBe true
    store.createOrAlterTable(Table("test_table", schema, Some("test_schema")))
  }

  override def afterAll() = {
    ds.close()
  }

  describe("The jdbc Catalog") {

    it("checks if a table exists") {
      store.tableExists(TableIdentifier("table")) shouldBe false
      store.tableExists(TableIdentifier("test_table")) shouldBe true
    }

    it("checks if a schema exists") {
      store.schemaExists("noschema") shouldBe false
      store.schemaExists("test_schema") shouldBe true
    }

    it("checks if a table with a schema exists") {
      store.tableExists(TableIdentifier("test_table", Some("test_schema"))) shouldBe true
      store.tableExists(TableIdentifier("table", Some("unknown"))) shouldBe false
    }

    it("errors if table exists") {
      intercept[UnableToCreateException] {
        store.createOrAlterTable(Table("test_table", schema, Some("test_schema")))
      }
    }

    it("errors if it can't create a table in a different database") {
      intercept[UnableToCreateException] {
        store.createOrAlterTable(Table("test_table", schema, Some("x")))
      }
    }

    it("validates table names") {
      store.validateName("test")
      intercept[AnalysisException] {
        store.validateName("!not-valid")
      }
    }

    it("gets existent tables") {
      store.getTableMetadata(TableIdentifier("unknown")).isFailure shouldBe true
      store.getTableMetadata(TableIdentifier("unknown")).isFailure shouldBe true
      intercept[NoSuchSchemaException] {
        store.getTableMetadata(TableIdentifier("unknown", Some("unknown")))
      }
      store.getTableMetadata(TableIdentifier("test_table", Some("test_schema"))).get shouldBe DbTable("test_table"
        , Seq.empty, None)
    }
  }

}
