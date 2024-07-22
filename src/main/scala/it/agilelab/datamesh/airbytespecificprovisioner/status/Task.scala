package it.agilelab.datamesh.airbytespecificprovisioner.status

import it.agilelab.datamesh.airbytespecificprovisioner.error.ErrorType
import it.agilelab.datamesh.airbytespecificprovisioner.model.ProvisionStatus.{Completed, Failed, Running}
import it.agilelab.datamesh.airbytespecificprovisioner.model.{ComponentToken, ProvisionResult, ProvisionStatus}
import it.agilelab.datamesh.airbytespecificprovisioner.status.TaskOperation.TaskOperation
import it.agilelab.datamesh.airbytespecificprovisioner.status.TaskStatus.TaskStatus

// Task doesn't include provisionerId against the LLD as we only are implement Async Provisioning V1
case class Task(id: ComponentToken, status: TaskStatus, operation: TaskOperation, info: Info) {

  def toProvisionResult: ProvisionResult =
    ProvisionResult(TaskStatus.toProvisioningStatus(status), id, info.errors, info.output)

}

object Task {

  def fromProvisionResult(result: ProvisionResult, taskOperation: TaskOperation): Task = Task(
    result.componentToken,
    TaskStatus.fromProvisioningStatus(result.provisioningStatus),
    taskOperation,
    Info(result.errors, result.output)
  )

}

object TaskStatus extends Enumeration {
  type TaskStatus = Value
  val WAITING, RUNNING, COMPLETED, FAILED = Value
  // where WAITING|RUNNING => RUNNING at API level

  def fromProvisioningStatus(status: ProvisionStatus): TaskStatus = status match {
    case Completed => COMPLETED
    case Running   => RUNNING
    case Failed    => FAILED
  }

  def toProvisioningStatus(status: TaskStatus): ProvisionStatus = status match {
    case WAITING | RUNNING => Running
    case COMPLETED         => Completed
    case FAILED            => Failed
  }

  def hasTerminated(status: TaskStatus): Boolean = status.equals(COMPLETED) || status.equals(FAILED)
}

object TaskOperation extends Enumeration {
  type TaskOperation = Value
  val VALIDATE, PROVISION, UNPROVISION, UPDATEACL = Value
}

case class Info(errors: Seq[ErrorType] = Seq.empty, output: String = "")
