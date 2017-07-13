package hydra.avro.sql

import java.util.Properties

import com.typesafe.config.ConfigFactory
import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import hydra.avro.util.JdbcHelper
import org.apache.avro.Schema
import org.scalatest.{FunSpecLike, Matchers}

/**
  * Created by alexsilva on 7/12/17.
  */
class JdbcCatalogSpec extends Matchers with FunSpecLike with JdbcHelper {

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

  describe("The jdbc store") {


    it("checks the db catalog for a table") {
      store.tableExists(TableIdentifier("table")) shouldBe false
    }

    it("creates a table") {
      val tblName = "table" + System.currentTimeMillis
      store.createTable(Table(tblName, schema))
      store.tableExists(TableIdentifier("table")) shouldBe true

    }
  }

}
