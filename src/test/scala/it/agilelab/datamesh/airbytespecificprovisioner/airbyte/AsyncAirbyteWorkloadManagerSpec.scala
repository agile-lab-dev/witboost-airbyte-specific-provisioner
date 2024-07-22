package it.agilelab.datamesh.airbytespecificprovisioner.airbyte

import cats.data.Validated.Valid
import io.circe.Json
import it.agilelab.datamesh.airbytespecificprovisioner.integrator.{AsyncAirbyteWorkloadManager, WorkloadManager}
import it.agilelab.datamesh.airbytespecificprovisioner.model._
import it.agilelab.datamesh.airbytespecificprovisioner.status.CacheRepository
import org.scalamock.scalatest.MockFactory
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

import scala.concurrent.ExecutionContext
import scala.concurrent.ExecutionContext.Implicits.global

class AsyncAirbyteWorkloadManagerSpec extends AnyFlatSpec with should.Matchers with MockFactory {

  "provisioning" should "store the task and return running result" in {
    val mockProvision  = stub[WorkloadManager]
    val repository     = new CacheRepository
    val asyncProvision = new AsyncAirbyteWorkloadManager(
      syncWorkloadManager = mockProvision,
      taskRepository = repository,
      executionContext = implicitly[ExecutionContext]
    )

    val yamlDescriptor = """
      dataProduct:
        dataProductOwner: user:name.surname_email.com
        devGroup: group:dev
        components:
          - kind: workload
            id: urn:dmb:cmp:healthcare:vaccinations-nb:0:airbyte-workload
            useCaseTemplateId: urn:dmb:utm:airbyte-standard:0.0.0
      componentIdToProvision: urn:dmb:cmp:healthcare:vaccinations-nb:0:airbyte-workload
      field1: "1"
      field2: "2"
      field3: "3"
    """

    val expectedResult = ProvisionResult.completed("OK")
    (mockProvision.provision _).when(COMPONENT_DESCRIPTOR, yamlDescriptor).returns(Valid(expectedResult))

    val result = asyncProvision.provision(COMPONENT_DESCRIPTOR, yamlDescriptor)

    result.isValid shouldEqual true
    result.map { result =>
      result.provisioningStatus shouldEqual ProvisionStatus.Running
      result.componentToken.isEmpty shouldBe false
    }
  }

  "unprovisioning" should "store the task and return running result" in {
    val mockProvision  = stub[WorkloadManager]
    val repository     = new CacheRepository
    val asyncProvision = new AsyncAirbyteWorkloadManager(
      syncWorkloadManager = mockProvision,
      taskRepository = repository,
      executionContext = implicitly[ExecutionContext]
    )

    val yamlDescriptor = """
      dataProduct:
        dataProductOwner: user:name.surname_email.com
        devGroup: group:dev
        components:
          - kind: workload
            id: urn:dmb:cmp:healthcare:vaccinations-nb:0:airbyte-workload
            useCaseTemplateId: urn:dmb:utm:airbyte-standard:0.0.0
      componentIdToProvision: urn:dmb:cmp:healthcare:vaccinations-nb:0:airbyte-workload
      field1: "1"
      field2: "2"
      field3: "3"
    """
    val expectedResult = ProvisionResult.completed()
    (mockProvision.unprovision _).when(COMPONENT_DESCRIPTOR, yamlDescriptor).returns(Valid(expectedResult))

    val result = asyncProvision.unprovision(COMPONENT_DESCRIPTOR, yamlDescriptor)

    result.map { result =>
      result.provisioningStatus shouldEqual ProvisionStatus.Running
      result.componentToken.isEmpty shouldBe false
    }
  }

  "validate" should "return immediately the validate result" in {
    val mockProvision  = mock[WorkloadManager]
    val repository     = new CacheRepository
    val asyncProvision = new AsyncAirbyteWorkloadManager(
      syncWorkloadManager = mockProvision,
      taskRepository = repository,
      executionContext = implicitly[ExecutionContext]
    )

    val yamlDescriptor = """
      dataProduct:
        components:
          - kind: workload
            id: urn:dmb:cmp:healthcare:vaccinations-nb:0:airbyte-workload
            useCaseTemplateId: urn:dmb:utm:airbyte-standard:0.0.0
      componentIdToProvision: urn:dmb:cmp:healthcare:vaccinations-nb:0:airbyte-workload
      some-field: 1
    """

    val expectedResult = Valid(AirbyteFields(
      dpFields = DataProductFields("name", "domain:domain", "0.0.0"),
      source = AirbyteSource("name", Json.obj(), Json.obj()),
      destination = AirbyteDestination("name", Json.obj(), Json.obj()),
      connection = AirbyteConnection("name", Json.obj())
    ))
    (mockProvision.validate _).expects(COMPONENT_DESCRIPTOR, yamlDescriptor).returns(expectedResult)

    val result = asyncProvision.validate(COMPONENT_DESCRIPTOR, yamlDescriptor)

    result shouldEqual expectedResult
  }
}
