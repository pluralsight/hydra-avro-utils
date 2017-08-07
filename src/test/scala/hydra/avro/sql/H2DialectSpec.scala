package hydra.avro.sql

import java.sql.JDBCType._

import org.apache.avro.Schema
import org.scalatest.{FunSpecLike, Matchers}

/**
  * Created by alexsilva on 5/4/17.
  */
class H2DialectSpec extends Matchers with FunSpecLike {
  val schema =
    """
      |{
      |	"type": "record",
      |	"name": "User",
      |	"namespace": "hydra",
      | "key":"id",
      |	"fields": [{
      |			"name": "id",
      |			"type": "int"
      |		},
      |		{
      |			"name": "username",
      |			"type": "string"
      |		},
      |		{
      |			"name": "active",
      |			"type": "boolean"
      |		}
      |	]
      |}
    """.stripMargin

  val avro = new Schema.Parser().parse(schema)

  describe("The H2 dialect") {

    it("handles h2db url") {
      H2Dialect.canHandle("jdbc:h2:mem:test;DB_CLOSE_DELAY=-1") shouldBe true
    }
    it("returns the correct types") {
      H2Dialect.getJDBCType(avro.getField("username").schema()).get shouldBe JdbcType("CLOB", CLOB)
      H2Dialect.getJDBCType(avro.getField("active").schema()) shouldBe Some(JdbcType("CHAR(1)", CHAR))
    }

    it("works with general sql commands") {
      H2Dialect.getTableExistsQuery("table") shouldBe "SELECT * FROM table WHERE 1=0"
      H2Dialect.getSchemaQuery("table") shouldBe "SELECT * FROM table WHERE 1=0"
    }

    it("returns upserts") {
      val upsert = "merge into table (\"id\",\"username\",\"active\") key(\"id\") values (?,?,?);"
      H2Dialect.upsert("table", avro, UnderscoreSyntax) shouldBe upsert
    }
  }
}
