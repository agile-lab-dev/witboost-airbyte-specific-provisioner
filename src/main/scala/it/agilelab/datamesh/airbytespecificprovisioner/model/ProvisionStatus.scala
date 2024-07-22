package it.agilelab.datamesh.airbytespecificprovisioner.model

/** Represents the status of a provisioning request.
 *
 *  This information is used when the request is executed in an asynchronous way.
 */
class ProvisionStatus {
  override def toString: String = ProvisionStatus.asString(this)
}

object ProvisionStatus {
  val Completed: ProvisionStatus = new ProvisionStatus
  val Failed: ProvisionStatus    = new ProvisionStatus
  val Running: ProvisionStatus   = new ProvisionStatus

  def asString(status: ProvisionStatus): String = status match {
    case Completed => "Completed"
    case Failed    => "Failed"
    case Running   => "Running"
    case _         => super.toString
  }
}
