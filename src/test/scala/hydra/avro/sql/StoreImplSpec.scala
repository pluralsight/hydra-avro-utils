package hydra.avro.sql

import com.typesafe.config.ConfigFactory
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.scalatest.{FunSpecLike, Matchers}

/**
  * Created by alexsilva on 7/12/17.
  */
class StoreImplSpec extends Matchers with FunSpecLike {

  val cfg = ConfigFactory.load().getConfig("db-cfg")
  val store = new StoreImpl(cfg)

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
    it("is configured") {
      store.hikariConfig.getDataSourceClassName shouldBe "org.h2.jdbcx.JdbcDataSource"
    }

    it("checks the db catalog for a table") {
      store.tableExists(TableIdentifier("table")) shouldBe false
    }

    it("creates a table") {
      val tblName = "table" + System.currentTimeMillis
      val table = store.createTable(Table(tblName, schema))
      val record = new GenericData.Record(schema)
      record.put("id", 1)
      record.put("username", "test")
      store.insertRecord(record, table)
    }
  }

}
