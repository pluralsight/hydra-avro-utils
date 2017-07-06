package hydra.avro.serde.jdbc

import java.sql.Types._

import org.apache.avro.Schema
import org.scalatest.{FunSpecLike, Matchers}

/**
  * Created by alexsilva on 5/4/17.
  */
class PostgresDialectSpec extends Matchers with FunSpecLike {
  val schema =
    """
      |{
      |	"type": "record",
      |	"name": "User",
      |	"namespace": "hydra",
      |	"fields": [{
      |			"name": "id",
      |			"type": "int",
      |     "meta":"primary-key"
      |		},
      |		{
      |			"name": "username",
      |			"type": "string"
      |		},
      |		{
      |			"name": "rate",
      |   "type": {
      |			"type": "bytes",
      |			"logicalType": "decimal",
      |			"precision": 4,
      |			"scale": 2
      |   }
      |		},
      |		{
      |			"name": "rateb",
      |			"type": "bytes"
      |		},
      |		{
      |			"name": "active",
      |			"type": "boolean",
      |      "doc": "active_doc"
      |		},
      |		{
      |			"name": "score",
      |			"type": "float"
      |		},
      |		{
      |			"name": "scored",
      |			"type": "double"
      |		},
      |		{
      |			"name": "passwordHash",
      |			"type": "bytes"
      |		},
      |		{
      |			"name": "signupTimestamp",
      |			"type": {
      |				"type": "long",
      |				"logicalType": "timestamp-millis"
      |			}
      |		},
      |		{
      |			"name": "signupDate",
      |			"type": {
      |				"type": "int",
      |				"logicalType": "date"
      |			}
      |		},
      |		{
      |			"name": "testUnion",
      |			"type": ["null", "string"]
      |		},
      |		{
      |			"name": "friends",
      |			"type": {
      |				"type": "array",
      |				"items": "string"
      |			}
      |		}
      |	]
      |}
    """.stripMargin


  describe("The postgres dialect") {
    it("converts a schema") {
      val avro = new Schema.Parser().parse(schema)
      PostgresDialect.getJDBCType(avro.getField("username").schema()).get shouldBe JdbcType("TEXT", CHAR)
      intercept[IllegalArgumentException] {
        PostgresDialect.getJDBCType(avro.getField("passwordHash").schema()).get shouldBe JdbcType("BYTEA", BINARY)
      }
      PostgresDialect.getJDBCType(avro.getField("rate").schema()) shouldBe Some(JdbcType("DECIMAL(4,2)", DECIMAL))
      PostgresDialect.getJDBCType(avro.getField("active").schema()) shouldBe Some(JdbcType("BOOLEAN", BOOLEAN))
      PostgresDialect.getJDBCType(avro.getField("score").schema()) shouldBe Some(JdbcType("FLOAT4", FLOAT))
      PostgresDialect.getJDBCType(avro.getField("scored").schema()) shouldBe Some(JdbcType("FLOAT8", DOUBLE))
      PostgresDialect.getJDBCType(avro.getField("testUnion").schema()) shouldBe Some(JdbcType("TEXT", CHAR))
      PostgresDialect.getJDBCType(avro.getField("friends").schema()) shouldBe Some(JdbcType("TEXT[]", ARRAY))
      PostgresDialect.getJDBCType(avro.getField("signupDate").schema()) shouldBe None
    }

    it("works with general sql commands") {
      PostgresDialect.getTableExistsQuery("table") shouldBe "SELECT 1 FROM table LIMIT 1"

      PostgresDialect.getSchemaQuery("table") shouldBe "SELECT * FROM table WHERE 1=0"
    }
  }
}
