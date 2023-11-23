package it.agilelab.datamesh.airbytespecificprovisioner.airbyte

import cats.data.Validated
import cats.data.Validated.Valid
import io.circe.Json
import it.agilelab.datamesh.airbytespecificprovisioner.common.Constants.{CONNECTION, DESTINATION, OPERATION, SOURCE}
import it.agilelab.datamesh.airbytespecificprovisioner.error.{
  GetConnectionInfoErrorType,
  GetIdFromCreationErrorType,
  InvalidResponse
}
import it.agilelab.datamesh.airbytespecificprovisioner.integrator.{AirbyteWorkloadManager, Client}
import it.agilelab.datamesh.airbytespecificprovisioner.model.{
  AirbyteConnection,
  AirbyteDestination,
  AirbyteFields,
  AirbyteSource,
  COMPONENT_DESCRIPTOR,
  DataProductFields
}
import it.agilelab.datamesh.airbytespecificprovisioner.validation.Validator
import org.scalamock.scalatest.MockFactory
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class AirbyteWorkloadManagerSpec extends AnyFlatSpec with MockFactory with Matchers {

  private val airbyteClient = mock[Client]
  private val validator     = mock[Validator]

  "Validation" should "succeed" in {
    val airbyteManager = new AirbyteWorkloadManager(validator, airbyteClient)
    val _              = (validator.validate _).expects(*, *).returns(Valid(AirbyteFields(
      DataProductFields("", "", ""),
      AirbyteSource("", Json.Null, Json.Null),
      AirbyteDestination("", Json.Null, Json.Null),
      AirbyteConnection("", Json.Null)
    )))

    val res = airbyteManager.validate(COMPONENT_DESCRIPTOR, "valid")

    res.isValid should be(true)
  }

  "Provision" should "succeed" in {
    val airbyteManager = new AirbyteWorkloadManager(validator, airbyteClient)
    val _              = (validator.validate _).expects(*, *).returns(Valid(AirbyteFields(
      DataProductFields("", "", ""),
      AirbyteSource("", Json.Null, Json.Null),
      AirbyteDestination("", Json.Null, Json.Null),
      AirbyteConnection("", Json.Null)
    )))
    val _ = (airbyteClient.deleteAndRecreate _).expects(*, *, *, SOURCE).returns(Right("""{"sourceId": "sourceId1"}"""))
    val _ = (airbyteClient.deleteAndRecreate _).expects(*, *, *, DESTINATION)
      .returns(Right("""{"destinationId": "destinationId1"}"""))
    val _ = (airbyteClient.create _).expects(*, *, OPERATION).returns(Right("""{"operationId": "operationId1"}"""))
    val _ = (airbyteClient.discoverSchema _).expects(*).returns(Right(
      """{"catalog":{"streams":[{"stream":{"name":"daily_electricity_consumption","jsonSchema":{"$schema":"http://json-schema.org/draft-07/schema#","type":"object","properties":{"date":{"type":["string","null"]},"daily_consumption_kwh":{"type":["number","null"]},"POD":{"type":["string","null"]},"id":{"type":["number","null"]}}},"supportedSyncModes":["full_refresh"],"defaultCursorField":[],"sourceDefinedPrimaryKey":[]},"config":{"syncMode":"full_refresh","cursorField":[],"destinationSyncMode":"append","primaryKey":[],"aliasName":"daily_electricity_consumption","selected":true,"suggested":true}}]},"jobInfo":{"id":"b8dd5de9-e269-42ee-a7d3-a7727e2c02c7","configType":"discover_schema","configId":"NoConfiguration","createdAt":0,"endedAt":0,"succeeded":true,"connectorConfigurationUpdated":false,"logs":{"logLines":[]}},"catalogId":"2072cd2d-195c-4530-8e1f-92364fc7de0b"}"""
    ))
    val _ = (airbyteClient.deleteAndRecreate _).expects(*, *, *, CONNECTION).returns(Right(""))

    val res = airbyteManager.provision(COMPONENT_DESCRIPTOR, "valid")

    res.isValid should be(true)
  }

  it should "fail if source creation fails" in {
    val airbyteManager = new AirbyteWorkloadManager(validator, airbyteClient)
    val _              = (validator.validate _).expects(*, *).returns(Valid(AirbyteFields(
      DataProductFields("", "", ""),
      AirbyteSource("", Json.Null, Json.Null),
      AirbyteDestination("", Json.Null, Json.Null),
      AirbyteConnection("", Json.Null)
    )))
    val _              = (airbyteClient.deleteAndRecreate _).expects(*, *, *, SOURCE).returns(Left(InvalidResponse("")))

    val res = airbyteManager.provision(COMPONENT_DESCRIPTOR, "valid")

    res match {
      case Validated.Valid(_)   => fail("Expected invalid")
      case Validated.Invalid(e) => e.head should be(InvalidResponse(""))
    }
  }

  it should "fail if destination creation fails" in {
    val airbyteManager = new AirbyteWorkloadManager(validator, airbyteClient)
    val _              = (validator.validate _).expects(*, *).returns(Valid(AirbyteFields(
      DataProductFields("", "", ""),
      AirbyteSource("", Json.Null, Json.Null),
      AirbyteDestination("", Json.Null, Json.Null),
      AirbyteConnection("", Json.Null)
    )))
    val _ = (airbyteClient.deleteAndRecreate _).expects(*, *, *, SOURCE).returns(Right("""{"sourceId": "sourceId1"}"""))
    val _ = (airbyteClient.deleteAndRecreate _).expects(*, *, *, DESTINATION).returns(Left(InvalidResponse("")))

    val res = airbyteManager.provision(COMPONENT_DESCRIPTOR, "valid")

    res match {
      case Validated.Valid(_)   => fail("Expected invalid")
      case Validated.Invalid(e) => e.head should be(InvalidResponse(""))
    }
  }

  it should "fail if operation creation fails" in {
    val airbyteManager = new AirbyteWorkloadManager(validator, airbyteClient)
    val _              = (validator.validate _).expects(*, *).returns(Valid(AirbyteFields(
      DataProductFields("", "", ""),
      AirbyteSource("", Json.Null, Json.Null),
      AirbyteDestination("", Json.Null, Json.Null),
      AirbyteConnection("", Json.Null)
    )))
    val _ = (airbyteClient.deleteAndRecreate _).expects(*, *, *, SOURCE).returns(Right("""{"sourceId": "sourceId1"}"""))
    val _ = (airbyteClient.deleteAndRecreate _).expects(*, *, *, DESTINATION)
      .returns(Right("""{"destinationId": "destinationId1"}"""))
    val _ = (airbyteClient.create _).expects(*, *, OPERATION).returns(Left(InvalidResponse("")))

    val res = airbyteManager.provision(COMPONENT_DESCRIPTOR, "valid")

    res match {
      case Validated.Valid(_)   => fail("Expected invalid")
      case Validated.Invalid(e) => e.head should be(InvalidResponse(""))
    }
  }

  it should "fail if source creation response is not as expected" in {
    val airbyteManager = new AirbyteWorkloadManager(validator, airbyteClient)
    val _              = (validator.validate _).expects(*, *).returns(Valid(AirbyteFields(
      DataProductFields("", "", ""),
      AirbyteSource("", Json.Null, Json.Null),
      AirbyteDestination("", Json.Null, Json.Null),
      AirbyteConnection("", Json.Null)
    )))
    val _              = (airbyteClient.deleteAndRecreate _).expects(*, *, *, SOURCE).returns(Right("{}"))
    val _              = (airbyteClient.deleteAndRecreate _).expects(*, *, *, DESTINATION)
      .returns(Right("""{"destinationId": "destinationId1"}"""))
    val _ = (airbyteClient.create _).expects(*, *, OPERATION).returns(Right("""{"operationId": "operationId1"}"""))

    val res = airbyteManager.provision(COMPONENT_DESCRIPTOR, "valid")

    res match {
      case Validated.Valid(_)   => fail("Expected invalid")
      case Validated.Invalid(e) => e.head should be(GetIdFromCreationErrorType(s"Failed to get sourceId from response"))
    }
  }

  it should "fail if destination creation response is not as expected" in {
    val airbyteManager = new AirbyteWorkloadManager(validator, airbyteClient)
    val _              = (validator.validate _).expects(*, *).returns(Valid(AirbyteFields(
      DataProductFields("", "", ""),
      AirbyteSource("", Json.Null, Json.Null),
      AirbyteDestination("", Json.Null, Json.Null),
      AirbyteConnection("", Json.Null)
    )))
    val _ = (airbyteClient.deleteAndRecreate _).expects(*, *, *, SOURCE).returns(Right("""{"sourceId": "sourceId1"}"""))
    val _ = (airbyteClient.deleteAndRecreate _).expects(*, *, *, DESTINATION).returns(Right("{}"))
    val _ = (airbyteClient.create _).expects(*, *, OPERATION).returns(Right("""{"operationId": "operationId1"}"""))

    val res = airbyteManager.provision(COMPONENT_DESCRIPTOR, "valid")

    res match {
      case Validated.Valid(_)   => fail("Expected invalid")
      case Validated.Invalid(e) => e.head should
          be(GetIdFromCreationErrorType(s"Failed to get destinationId from response"))
    }
  }

  it should "fail if operation creation response is not as expected" in {
    val airbyteManager = new AirbyteWorkloadManager(validator, airbyteClient)
    val _              = (validator.validate _).expects(*, *).returns(Valid(AirbyteFields(
      DataProductFields("", "", ""),
      AirbyteSource("", Json.Null, Json.Null),
      AirbyteDestination("", Json.Null, Json.Null),
      AirbyteConnection("", Json.Null)
    )))
    val _ = (airbyteClient.deleteAndRecreate _).expects(*, *, *, SOURCE).returns(Right("""{"sourceId": "sourceId1"}"""))
    val _ = (airbyteClient.deleteAndRecreate _).expects(*, *, *, DESTINATION)
      .returns(Right("""{"destinationId": "destinationId1"}"""))
    val _ = (airbyteClient.create _).expects(*, *, OPERATION).returns(Right("{}"))

    val res = airbyteManager.provision(COMPONENT_DESCRIPTOR, "valid")

    res match {
      case Validated.Valid(_)   => fail("Expected invalid")
      case Validated.Invalid(e) => e.head should
          be(GetIdFromCreationErrorType(s"Failed to get operationId from response"))
    }
  }

  it should "fail if discover schema fails" in {
    val airbyteManager = new AirbyteWorkloadManager(validator, airbyteClient)
    val _              = (validator.validate _).expects(*, *).returns(Valid(AirbyteFields(
      DataProductFields("", "", ""),
      AirbyteSource("", Json.Null, Json.Null),
      AirbyteDestination("", Json.Null, Json.Null),
      AirbyteConnection("", Json.Null)
    )))
    val _ = (airbyteClient.deleteAndRecreate _).expects(*, *, *, SOURCE).returns(Right("""{"sourceId": "sourceId1"}"""))
    val _ = (airbyteClient.deleteAndRecreate _).expects(*, *, *, DESTINATION)
      .returns(Right("""{"destinationId": "destinationId1"}"""))
    val _ = (airbyteClient.create _).expects(*, *, OPERATION).returns(Right("""{"operationId": "operationId1"}"""))
    val _ = (airbyteClient.discoverSchema _).expects(*).returns(Left(InvalidResponse("")))

    val res = airbyteManager.provision(COMPONENT_DESCRIPTOR, "valid")

    res match {
      case Validated.Valid(_)   => fail("Expected invalid")
      case Validated.Invalid(e) => e.head should be(InvalidResponse(""))
    }
  }

  it should "fail if discover schema response is not as expected" in {
    val airbyteManager = new AirbyteWorkloadManager(validator, airbyteClient)
    val _              = (validator.validate _).expects(*, *).returns(Valid(AirbyteFields(
      DataProductFields("", "", ""),
      AirbyteSource("", Json.Null, Json.Null),
      AirbyteDestination("", Json.Null, Json.Null),
      AirbyteConnection("", Json.Null)
    )))
    val _ = (airbyteClient.deleteAndRecreate _).expects(*, *, *, SOURCE).returns(Right("""{"sourceId": "sourceId1"}"""))
    val _ = (airbyteClient.deleteAndRecreate _).expects(*, *, *, DESTINATION)
      .returns(Right("""{"destinationId": "destinationId1"}"""))
    val _ = (airbyteClient.create _).expects(*, *, OPERATION).returns(Right("""{"operationId": "operationId1"}"""))
    val _ = (airbyteClient.discoverSchema _).expects(*).returns(Right("{}"))

    val res = airbyteManager.provision(COMPONENT_DESCRIPTOR, "valid")

    res match {
      case Validated.Valid(_)   => fail("Expected invalid")
      case Validated.Invalid(e) => e.head should be(GetConnectionInfoErrorType(s"Failed to get catalog from response"))
    }
  }

  it should "fail if connection creation fails" in {
    val airbyteManager = new AirbyteWorkloadManager(validator, airbyteClient)
    val _              = (validator.validate _).expects(*, *).returns(Valid(AirbyteFields(
      DataProductFields("", "", ""),
      AirbyteSource("", Json.Null, Json.Null),
      AirbyteDestination("", Json.Null, Json.Null),
      AirbyteConnection("", Json.Null)
    )))
    val _ = (airbyteClient.deleteAndRecreate _).expects(*, *, *, SOURCE).returns(Right("""{"sourceId": "sourceId1"}"""))
    val _ = (airbyteClient.deleteAndRecreate _).expects(*, *, *, DESTINATION)
      .returns(Right("""{"destinationId": "destinationId1"}"""))
    val _ = (airbyteClient.create _).expects(*, *, OPERATION).returns(Right("""{"operationId": "operationId1"}"""))
    val _ = (airbyteClient.discoverSchema _).expects(*).returns(Right(
      """{"catalog":{"streams":[{"stream":{"name":"daily_electricity_consumption","jsonSchema":{"$schema":"http://json-schema.org/draft-07/schema#","type":"object","properties":{"date":{"type":["string","null"]},"daily_consumption_kwh":{"type":["number","null"]},"POD":{"type":["string","null"]},"id":{"type":["number","null"]}}},"supportedSyncModes":["full_refresh"],"defaultCursorField":[],"sourceDefinedPrimaryKey":[]},"config":{"syncMode":"full_refresh","cursorField":[],"destinationSyncMode":"append","primaryKey":[],"aliasName":"daily_electricity_consumption","selected":true,"suggested":true}}]},"jobInfo":{"id":"b8dd5de9-e269-42ee-a7d3-a7727e2c02c7","configType":"discover_schema","configId":"NoConfiguration","createdAt":0,"endedAt":0,"succeeded":true,"connectorConfigurationUpdated":false,"logs":{"logLines":[]}},"catalogId":"2072cd2d-195c-4530-8e1f-92364fc7de0b"}"""
    ))
    val _ = (airbyteClient.deleteAndRecreate _).expects(*, *, *, CONNECTION).returns(Left(InvalidResponse("")))

    val res = airbyteManager.provision(COMPONENT_DESCRIPTOR, "valid")

    res match {
      case Validated.Valid(_)   => fail("Expected invalid")
      case Validated.Invalid(e) => e.head should be(InvalidResponse(""))
    }
  }

  "Unprovision" should "succeed" in {
    val airbyteManager = new AirbyteWorkloadManager(validator, airbyteClient)
    val _              = (validator.validate _).expects(*, *).returns(Valid(AirbyteFields(
      DataProductFields("", "", ""),
      AirbyteSource("", Json.Null, Json.Null),
      AirbyteDestination("", Json.Null, Json.Null),
      AirbyteConnection("", Json.Null)
    )))
    val _              = (airbyteClient.delete _).expects(*, *, CONNECTION).returns(Right(()))
    val _              = (airbyteClient.delete _).expects(*, *, DESTINATION).returns(Right(()))
    val _              = (airbyteClient.delete _).expects(*, *, SOURCE).returns(Right(()))

    val res = airbyteManager.unprovision(COMPONENT_DESCRIPTOR, "valid")

    res.isValid should be(true)
  }

  it should "fail if connection deletion fails" in {
    val airbyteManager = new AirbyteWorkloadManager(validator, airbyteClient)
    val _              = (validator.validate _).expects(*, *).returns(Valid(AirbyteFields(
      DataProductFields("", "", ""),
      AirbyteSource("", Json.Null, Json.Null),
      AirbyteDestination("", Json.Null, Json.Null),
      AirbyteConnection("", Json.Null)
    )))
    val _              = (airbyteClient.delete _).expects(*, *, CONNECTION).returns(Left(InvalidResponse("")))

    val res = airbyteManager.unprovision(COMPONENT_DESCRIPTOR, "valid")

    res match {
      case Validated.Valid(_)   => fail("Expected invalid")
      case Validated.Invalid(e) => e.head should be(InvalidResponse(""))
    }
  }

  it should "fail if destination deletion fails" in {
    val airbyteManager = new AirbyteWorkloadManager(validator, airbyteClient)
    val _              = (validator.validate _).expects(*, *).returns(Valid(AirbyteFields(
      DataProductFields("", "", ""),
      AirbyteSource("", Json.Null, Json.Null),
      AirbyteDestination("", Json.Null, Json.Null),
      AirbyteConnection("", Json.Null)
    )))
    val _              = (airbyteClient.delete _).expects(*, *, CONNECTION).returns(Right(()))
    val _              = (airbyteClient.delete _).expects(*, *, DESTINATION).returns(Left(InvalidResponse("")))
    val _              = (airbyteClient.delete _).expects(*, *, SOURCE).returns(Right(()))

    val res = airbyteManager.unprovision(COMPONENT_DESCRIPTOR, "valid")

    res match {
      case Validated.Valid(_)   => fail("Expected invalid")
      case Validated.Invalid(e) => e.head should be(InvalidResponse(""))
    }
  }

  it should "fail if source deletion fails" in {
    val airbyteManager = new AirbyteWorkloadManager(validator, airbyteClient)
    val _              = (validator.validate _).expects(*, *).returns(Valid(AirbyteFields(
      DataProductFields("", "", ""),
      AirbyteSource("", Json.Null, Json.Null),
      AirbyteDestination("", Json.Null, Json.Null),
      AirbyteConnection("", Json.Null)
    )))
    val _              = (airbyteClient.delete _).expects(*, *, CONNECTION).returns(Right(()))
    val _              = (airbyteClient.delete _).expects(*, *, DESTINATION).returns(Right(()))
    val _              = (airbyteClient.delete _).expects(*, *, SOURCE).returns(Left(InvalidResponse("")))

    val res = airbyteManager.unprovision(COMPONENT_DESCRIPTOR, "valid")

    res match {
      case Validated.Valid(_)   => fail("Expected invalid")
      case Validated.Invalid(e) => e.head should be(InvalidResponse(""))
    }
  }

}
