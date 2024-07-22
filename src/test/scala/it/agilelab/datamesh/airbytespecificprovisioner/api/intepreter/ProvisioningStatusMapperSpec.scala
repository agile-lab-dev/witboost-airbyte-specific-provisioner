package it.agilelab.datamesh.airbytespecificprovisioner.api.intepreter

import it.agilelab.datamesh.airbytespecificprovisioner.error.InvalidComponent
import it.agilelab.datamesh.airbytespecificprovisioner.model.{
  ComponentToken,
  ProvisionResult,
  ProvisionStatus,
  ProvisioningStatusEnums
}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

class ProvisioningStatusMapperSpec extends AnyFlatSpec with should.Matchers {

  List(
    ProvisionStatus.Completed -> ProvisioningStatusEnums.StatusEnum.COMPLETED,
    ProvisionStatus.Failed    -> ProvisioningStatusEnums.StatusEnum.FAILED,
    ProvisionStatus.Running   -> ProvisioningStatusEnums.StatusEnum.RUNNING
  ).foreach { case (from, expected) =>
    it should s"transform $from into $expected" in { ProvisioningStatusMapper.from(from).status shouldEqual expected }
  }

  it should "transform a Failed ProvisionResult" in {
    val from   = ProvisionResult(ProvisionStatus.Failed, ComponentToken(""), errors = List(InvalidComponent("Error")))
    val actual = ProvisioningStatusMapper.from(from)

    actual.status shouldEqual ProvisioningStatusEnums.StatusEnum.FAILED
    actual.result shouldEqual "Component with ID 'Error' is not of type workload"
  }

}
