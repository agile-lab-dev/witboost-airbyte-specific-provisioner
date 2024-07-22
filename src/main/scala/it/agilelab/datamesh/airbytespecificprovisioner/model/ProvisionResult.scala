package it.agilelab.datamesh.airbytespecificprovisioner.model

import it.agilelab.datamesh.airbytespecificprovisioner.error.ErrorType

object ProvisionResult {

  def completed(): ProvisionResult = completed(ComponentToken(""), output = "")

  def completed(output: String): ProvisionResult = completed(ComponentToken(""), output = output)

  def completed(componentToken: ComponentToken, output: String): ProvisionResult =
    ProvisionResult(ProvisionStatus.Completed, componentToken, Seq.empty, output)

  def failure(errors: Seq[ErrorType]): ProvisionResult =
    ProvisionResult(ProvisionStatus.Failed, ComponentToken(""), errors)

  def running(token: ComponentToken): ProvisionResult = ProvisionResult(ProvisionStatus.Running, token, Seq.empty)
}

/** This class is the result of a provisioning operation.
 *
 *  It can represent any of the following three cases:
 *  - completed: the provision has been performed synchronously and successfully
 *  - failed: the provision has been performed synchronously but unsuccessfully
 *  - running: the provision will be performed asynchronously and a token is returned
 */
case class ProvisionResult(
    provisioningStatus: ProvisionStatus,
    componentToken: ComponentToken,
    errors: Seq[ErrorType],
    output: String = ""
) {

  /** Returns true if the provisioning of the component is successful. */
  def isSuccessful: Boolean = provisioningStatus.equals(ProvisionStatus.Failed)
}
