package it.agilelab.datamesh.airbytespecificprovisioner.status

import it.agilelab.datamesh.airbytespecificprovisioner.model.{ComponentToken, ProvisionResult}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

class GetStatusSpec extends AnyFlatSpec with should.Matchers {

  "alwaysCompletedStatus" should "always return a completed ProvisionResult" in {
    val getStatus: GetStatus = GetStatus.alwaysCompletedStatus

    getStatus.statusOf(ComponentToken("any-token")) shouldEqual Some(ProvisionResult.completed())
  }
}
