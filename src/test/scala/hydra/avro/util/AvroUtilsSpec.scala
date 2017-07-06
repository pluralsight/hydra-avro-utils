package hydra.avro.util

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
  }
}
