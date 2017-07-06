package hydra.avro.serde.jdbc

import org.scalatest.{FunSpecLike, Matchers}

/**
  * Created by alexsilva on 5/4/17.
  */
class NoOpDIalectSpec extends Matchers with FunSpecLike {


  describe("The NoOp dialect") {
    it("handles everything") {
      NoopDialect.canHandle("url") shouldBe true
    }
  }
}
