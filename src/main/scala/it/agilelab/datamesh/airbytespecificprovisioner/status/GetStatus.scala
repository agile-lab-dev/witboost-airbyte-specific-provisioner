package it.agilelab.datamesh.airbytespecificprovisioner.status

import it.agilelab.datamesh.airbytespecificprovisioner.model.{ComponentToken, ProvisionResult}

/** This trait represents the provisioning status
 *  of the request which is identified by an input token.
 *  *
 *  If the provision operation always executes synchronous task,
 *  this trait is meaningless and is meant to never be invoked.
 */
trait GetStatus {

  /** Analyzes the token, extracts the necessary
   *  information from it and uses them to check for the outcome of a
   *  specific provisioning operation
   *
   *  @param token an identifier of the component
   *  @return the status of the provisioning request
   */
  def statusOf(token: ComponentToken): Option[ProvisionResult]
}

object GetStatus {
  def alwaysCompletedStatus: GetStatus = _ => Some(ProvisionResult.completed())
}
