package mesosphere.marathon
package integration

import mesosphere.AkkaIntegrationTest
import mesosphere.marathon.integration.setup.EmbeddedMarathonTest
import mesosphere.marathon.raml.GroupUpdate
import play.api.libs.json.{JsObject, Json}

class MetricsIntegrationTest extends AkkaIntegrationTest with EmbeddedMarathonTest {

  "Marathon Metrics" should {
    "correctly count outgoing HTTP bytes" in {

      When("The metrics endpoint is queried")
      val result = marathon.metrics()

      Then("The system responds as expected")
      result should be(OK)
      result.entityJson.as[JsObject].keys should contain("counters")
      result.entityJson("counters").as[JsObject].keys should contain("marathon.http.data.written.bytes.total")

      And("The `outputBytes` is increased as expected")
      val currentCounter = result.entityJson("counters")("marathon.http.data.written.bytes.total")("count").as[Int]

      // Give some time to Kamon to take a metrics snapshot.
      Thread.sleep(3000)

      val newResult = marathon.metrics()
      val newCounter = newResult.entityJson("counters")("marathon.http.data.written.bytes.total")("count").as[Int]
      newCounter shouldBe >=(currentCounter + result.entityString.length)

    }

    "correctly count incoming HTTP bytes" in {

      When("The metrics endpoint is queried")
      val result = marathon.metrics()

      Then("The system responds as expected")
      result should be(OK)
      result.entityJson.as[JsObject].keys should contain("counters")
      result.entityJson("counters").as[JsObject].keys should contain("marathon.http.data.read.bytes.total")

      And("The `inputBytes` is increased as expected")
      val currentCounter = result.entityJson("counters")("marathon.http.data.read.bytes.total")("count").as[Int]
      val requestObj = GroupUpdate(id = Some("/empty"))
      val requestJson = Json.toJson(requestObj).toString()
      marathon.createGroup(requestObj)

      // Give some time to Kamon to take a metrics snapshot.
      Thread.sleep(3000)

      val newResult = marathon.metrics()
      val newCounter = newResult.entityJson("counters")("marathon.http.data.read.bytes.total")("count").as[Int]
      newCounter shouldBe >=(currentCounter + requestJson.length)

    }
  }

}
