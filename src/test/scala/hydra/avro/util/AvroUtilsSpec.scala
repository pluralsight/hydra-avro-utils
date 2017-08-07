package hydra.avro.util

import org.apache.avro.Schema
import org.apache.avro.Schema.Field
import org.scalatest.{FunSpecLike, Matchers}

/**
  * Created by alexsilva on 7/6/17.
  */
class AvroUtilsSpec extends Matchers with FunSpecLike {

  describe("When using AvroUtils") {
    it("replaces invalid characters") {
      AvroUtils.cleanName("!test") shouldBe "_test"
      AvroUtils.cleanName("?test") shouldBe "_test"
      AvroUtils.cleanName("_test") shouldBe "_test"
      AvroUtils.cleanName("test") shouldBe "test"
      AvroUtils.cleanName("1test") shouldBe "1test"
    }

    it("Gets a schema field by name") {
      val schema =
        """
          |{
          |	"type": "record",
          |	"name": "User",
          |	"namespace": "hydra",
          |	"fields": [
          |		{
          |			"name": "testEnum",
          |			"type": {
          |            "type": "enum",
          |            "name": "test_type",
          |            "symbols": ["test1", "test2"]
          |        }
          |		}
          |	]
          |}
        """.stripMargin


      val avro = new Schema.Parser().parse(schema)

      AvroUtils.getField("testEnum", avro) shouldBe avro.getField("testEnum")
      intercept[IllegalArgumentException] {
        AvroUtils.getField("unknown", avro)
      }
    }

    it("throws exception if primary key doesn't exist") {
      val schema =
        """
          |{
          |	"type": "record",
          |	"name": "User",
          |	"namespace": "hydra",
          | "key":"name",
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

      val avro = new Schema.Parser().parse(schema)

      intercept[IllegalArgumentException] {
        AvroUtils.getPrimaryKeys(avro) shouldBe None
      }
    }

    it("returns a single primary key") {
      val schema =
        """
          |{
          |	"type": "record",
          |	"name": "User",
          |	"namespace": "hydra",
          | "key":"id",
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

      val avro = new Schema.Parser().parse(schema)

      AvroUtils.getPrimaryKeys(avro) shouldBe Seq(avro.getField("id"))
    }

    it("allows primary keys to be supplied") {
      val schema =
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

      val avro = new Schema.Parser().parse(schema)

      AvroUtils.getPrimaryKeys(avro) shouldBe Seq.empty[Field]
    }

    it("returns a composite primary key") {
      val schema =
        """
          |{
          |	"type": "record",
          |	"name": "User",
          |	"namespace": "hydra",
          | "key":"id1,id2",
          |	"fields": [{
          |			"name": "id1",
          |			"type": "int",
          |			"doc": "doc"
          |		},
          |  {
          |			"name": "id2",
          |			"type": "int",
          |			"doc": "doc"
          |		},
          |		{
          |			"name": "username",
          |			"type": ["null", "string"]
          |		}
          |	]
          |}""".stripMargin

      val avro = new Schema.Parser().parse(schema)

      AvroUtils.getPrimaryKeys(avro) shouldBe Seq(avro.getField("id1"), avro.getField("id2"))

    }
  }
}
