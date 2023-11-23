package it.agilelab.datamesh.airbytespecificprovisioner.integrator

import io.circe.Json
import it.agilelab.datamesh.airbytespecificprovisioner.error.AirbyteClientErrorType

trait Client {

  def create(workspaceId: String, jsonRequest: Json, resourceType: String): Either[AirbyteClientErrorType, String]

  def deleteAndRecreate(
      workspaceId: String,
      jsonRequest: Json,
      resourceName: String,
      resourceType: String
  ): Either[AirbyteClientErrorType, String]
  def delete(workspaceId: String, resourceName: String, resourceType: String): Either[AirbyteClientErrorType, Unit]
  def discoverSchema(sourceId: String): Either[AirbyteClientErrorType, String]

}
