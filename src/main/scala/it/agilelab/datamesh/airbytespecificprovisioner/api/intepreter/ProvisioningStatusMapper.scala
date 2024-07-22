package it.agilelab.datamesh.airbytespecificprovisioner.api.intepreter

import it.agilelab.datamesh.airbytespecificprovisioner.model.ProvisionStatus.{Completed, Failed, Running}
import it.agilelab.datamesh.airbytespecificprovisioner.model.{
  ProvisionResult,
  ProvisionStatus,
  ProvisioningStatus,
  ProvisioningStatusEnums
}

object ProvisioningStatusMapper {

  def from(result: ProvisionResult): ProvisioningStatus = result.provisioningStatus match {
    case Completed => ProvisioningStatus(ProvisioningStatusEnums.StatusEnum.COMPLETED, "OK")
    case Failed    =>
      ProvisioningStatus(ProvisioningStatusEnums.StatusEnum.FAILED, result.errors.map(_.errorMessage).mkString(","))
    case Running   => ProvisioningStatus(ProvisioningStatusEnums.StatusEnum.RUNNING, "")
  }

  def from(status: ProvisionStatus): ProvisioningStatus = status match {
    case Completed => ProvisioningStatus(ProvisioningStatusEnums.StatusEnum.COMPLETED, "OK")
    case Failed    => ProvisioningStatus(ProvisioningStatusEnums.StatusEnum.FAILED, "")
    case Running   => ProvisioningStatus(ProvisioningStatusEnums.StatusEnum.RUNNING, "")
  }
}
