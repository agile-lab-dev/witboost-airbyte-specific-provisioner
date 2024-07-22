package it.agilelab.datamesh.airbytespecificprovisioner.status

import it.agilelab.datamesh.airbytespecificprovisioner.error.AsyncProvisionFailure
import it.agilelab.datamesh.airbytespecificprovisioner.model.{ComponentToken, ProvisionResult, ProvisionStatus}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

class TaskSpec extends AnyFlatSpec with should.Matchers {

  it should "transform a Task into a ProvisionResult" in {
    val token  = ComponentToken("token")
    val errors = List(AsyncProvisionFailure("Error!", new Exception("Error!")))
    val task   =
      Task(id = token, status = TaskStatus.FAILED, operation = TaskOperation.PROVISION, info = Info(errors = errors))

    val expected = ProvisionResult(provisioningStatus = ProvisionStatus.Failed, componentToken = token, errors = errors)

    task.toProvisionResult shouldEqual expected

  }

  it should "transform a ProvisionResult into a Task" in {
    val token   = ComponentToken("token")
    val outputs = "airbyte-output"

    val provisionResult = ProvisionResult(
      provisioningStatus = ProvisionStatus.Completed,
      componentToken = token,
      errors = Seq.empty,
      output = outputs
    )

    val expected = Task(
      id = token,
      status = TaskStatus.COMPLETED,
      operation = TaskOperation.UNPROVISION,
      info = Info(output = outputs)
    )

    Task.fromProvisionResult(provisionResult, TaskOperation.UNPROVISION) shouldEqual expected
  }

}
