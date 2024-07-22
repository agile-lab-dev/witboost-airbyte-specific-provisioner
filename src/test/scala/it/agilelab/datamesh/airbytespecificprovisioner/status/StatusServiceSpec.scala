package it.agilelab.datamesh.airbytespecificprovisioner.status

import it.agilelab.datamesh.airbytespecificprovisioner.model.{ComponentToken, ProvisionStatus}
import org.scalamock.scalatest.MockFactory
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

class StatusServiceSpec extends AnyFlatSpec with should.Matchers with MockFactory {

  "StatusService" should "return Some ProvisionResult if present on repo" in {
    val repository = mock[TaskRepository]

    val statusService = new StatusService(repository)
    val token         = ComponentToken("a-token")
    (repository.findTask _).expects(token)
      .returns(Some(Task(id = token, status = TaskStatus.RUNNING, operation = TaskOperation.PROVISION, info = Info())))

    val actual = statusService.statusOf(token)

    actual shouldNot be(None)
    actual.get.componentToken shouldEqual token
    actual.get.provisioningStatus shouldEqual ProvisionStatus.Running
    actual.get.errors shouldBe empty
    actual.get.output shouldBe empty
  }

  it should "return None if Task not present in repo" in {
    val repository = mock[TaskRepository]

    val statusService = new StatusService(repository)
    val token         = ComponentToken("a-token")
    (repository.findTask _).expects(token).returns(None)

    val actual = statusService.statusOf(token)

    actual shouldBe None
  }

}
