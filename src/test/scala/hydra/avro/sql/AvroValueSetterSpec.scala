package hydra.avro.sql

import java.math.{MathContext, RoundingMode}
import java.sql.{Connection, Date, PreparedStatement, Timestamp}
import java.time.{LocalDate, ZoneId}

import com.google.common.collect.Lists
import com.pluralsight.hydra.avro.sql.MockArray
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.scalamock.scalatest.MockFactory
import org.scalatest.{FunSpecLike, Matchers}

/**
  * Created by alexsilva on 5/4/17.
  */
class ValueSetterSpec extends Matchers with FunSpecLike with MockFactory {
  val schemaStr =
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
      |  		{
      |			"name": "testNullUnion",
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

  val schema = new Schema.Parser().parse(schemaStr)

  val valueSetter = new AvroValueSetter(schema, PostgresDialect, UnderscoreSyntax)
  describe("The AvroValueSetter") {
    it("sets values") {
      val ts = System.currentTimeMillis
      val ctx = new MathContext(4, RoundingMode.HALF_EVEN)
      val decimal = new java.math.BigDecimal("0.2", ctx).setScale(2)
      val dt = LocalDate.ofEpochDay(1234).atStartOfDay(ZoneId.systemDefault()).toInstant.toEpochMilli
      val mockedStmt = mock[PreparedStatement]
      val connection = mock[Connection]
      val friends = Lists.newArrayList("friend1", "friend2")
      (mockedStmt.getConnection _).expects().returning(connection)
      val mockArray = new MockArray(friends)
      (connection.createArrayOf(_, _)).expects("CHAR", *).returning(mockArray)
      (mockedStmt.setInt _).expects(1, 1)
      (mockedStmt.setString _).expects(2, "alex")
      (mockedStmt.setBigDecimal _).expects(3, decimal)
      (mockedStmt.setBoolean _).expects(4, true)
      (mockedStmt.setFloat _).expects(5, 10f)
      (mockedStmt.setDouble _).expects(6, 2.5d)
      (mockedStmt.setTimestamp(_: Int, _: Timestamp)).expects(7, new Timestamp(ts))
      (mockedStmt.setDate(_: Int, _: Date)).expects(8, new Date(dt))
      (mockedStmt.setString _).expects(9, "test")
      (mockedStmt.setNull(_: Int, _: Int)).expects(10, java.sql.Types.CHAR)
      (mockedStmt.setArray _).expects(11, mockArray)

      val record = new GenericData.Record(schema)
      record.put("id", 1)
      record.put("username", "alex")
      record.put("rate", 0.2d)
      record.put("active", true)
      record.put("score", 10f)
      record.put("scored", 2.5d)
      record.put("signupTimestamp", ts)
      record.put("signupDate", 1234)
      record.put("friends", new GenericData.Array[String](schema.getField("friends").schema(), friends))
      record.put("testUnion", "test")
      record.put("testNullUnion", null)
      valueSetter.setValues(record, mockedStmt)
    }
  }
}