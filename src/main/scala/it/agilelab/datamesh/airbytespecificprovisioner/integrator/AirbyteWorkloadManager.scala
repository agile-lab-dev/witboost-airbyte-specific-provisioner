package it.agilelab.datamesh.airbytespecificprovisioner.integrator

import com.typesafe.scalalogging.StrictLogging
import io.circe.syntax.EncoderOps
import io.circe.{parser, Json}
import it.agilelab.datamesh.airbytespecificprovisioner.common.Constants.{CONNECTION, DESTINATION, SOURCE}
import it.agilelab.datamesh.airbytespecificprovisioner.descriptor.{ComponentDescriptor, ComponentExtractor}
import it.agilelab.datamesh.airbytespecificprovisioner.model.{
  COMPONENT_DESCRIPTOR,
  DescriptorKind,
  SystemError,
  ValidationError
}

class AirbyteWorkloadManager(airbyteClient: AirbyteClient) extends StrictLogging {

  private def getIdFromCreationResponse(creationResponse: String, field: String): Either[Product, String] = {
    val creationResponseJson = parser.parse(creationResponse).getOrElse(Json.Null)
    creationResponseJson.hcursor.downField(field).as[String].left
      .map(_ => SystemError(s"Failed to get $field from response"))
  }

  private def provisionSource(componentDescriptor: ComponentDescriptor): Either[Product, String] = for {
    workspaceId            <- componentDescriptor.getComponentWorkspaceId
    source                 <- componentDescriptor.getComponentSource
    sourceCreationResponse <- airbyteClient
      .createOrRecreate(workspaceId, source.deepMerge(Json.obj(("workspaceId", Json.fromString(workspaceId)))), SOURCE)
  } yield sourceCreationResponse

  private def provisionDestination(componentDescriptor: ComponentDescriptor): Either[Product, String] = for {
    workspaceId                 <- componentDescriptor.getComponentWorkspaceId
    destination                 <- componentDescriptor.getComponentDestination
    destinationCreationResponse <- airbyteClient.createOrRecreate(
      workspaceId,
      destination.deepMerge(Json.obj(("workspaceId", Json.fromString(workspaceId)))),
      DESTINATION
    )
  } yield destinationCreationResponse

  private def provisionConnection(
      componentDescriptor: ComponentDescriptor,
      connectionInfo: Json,
      provisionedSourceId: String,
      provisionedDestinationId: String
  ): Either[Product, String] = for {
    workspaceId                 <- componentDescriptor.getComponentWorkspaceId
    componentConnection         <- componentDescriptor.getComponentConnection
    connectionCreationResponses <- airbyteClient.createOrRecreate(
      workspaceId,
      componentConnection.deepMerge(Json.obj(
        ("syncCatalog", connectionInfo),
        ("sourceId", Json.fromString(provisionedSourceId)),
        ("destinationId", Json.fromString(provisionedDestinationId))
      )),
      CONNECTION
    )
  } yield connectionCreationResponses

  private def getConnectionInfo(schemaResponse: String): Either[Product, Json] = {
    val schemaResponseJson = parser.parse(schemaResponse).getOrElse(Json.Null)
    schemaResponseJson.hcursor.downField("catalog").as[Json] match {
      case Left(_)        => Left(SystemError(s"Failed to get catalog from response"))
      case Right(catalog) => catalog.hcursor.downField("streams").downArray.as[Json] match {
          case Left(_)            => Left(SystemError(s"Failed to get streams from response"))
          case Right(streamsJson) =>
            val filteredStreams: Option[Json] = streamsJson.hcursor.downField("stream").downField("jsonSchema")
              .withFocus(_.asObject.map(_.filterKeys(!_.equals("$schema")).asJson).get).top
            filteredStreams match {
              case Some(value) =>
                val updatedStreams = value.hcursor.downField("config").downField("destinationSyncMode")
                  .withFocus(_.mapString(_ => "overwrite")).top
                updatedStreams match {
                  case Some(value) => Right(catalog.deepMerge(Json.obj(("streams", Json.arr(value)))))
                  case _           => Left(SystemError("Unable to process stream config"))
                }
              case _           => Left(SystemError("Unable to process stream jsonSchema"))
            }
        }
    }
  }

  private def provisionComponent(componentDescriptor: ComponentDescriptor): Either[Product, String] = for {
    provisionedSourceResponse      <- provisionSource(componentDescriptor)
    provisionedDestinationResponse <- provisionDestination(componentDescriptor)
    provisionedSourceId            <- getIdFromCreationResponse(provisionedSourceResponse, "sourceId")
    provisionedDestinationId       <- getIdFromCreationResponse(provisionedDestinationResponse, "destinationId")
    discoveredSchemaResponse       <- airbyteClient.discoverSchema(provisionedSourceId)
    connectionInfo                 <- getConnectionInfo(discoveredSchemaResponse)
    provisionedConnections         <-
      provisionConnection(componentDescriptor, connectionInfo, provisionedSourceId, provisionedDestinationId)
  } yield provisionedConnections

  private def unprovisionResource(
      componentDescriptor: ComponentDescriptor,
      resourceType: String
  ): Either[Product, String] = for {
    workspaceId            <- componentDescriptor.getComponentWorkspaceId
    resource               <- resourceType match {
      case SOURCE      => componentDescriptor.getComponentSource
      case DESTINATION => componentDescriptor.getComponentDestination
      case CONNECTION  => componentDescriptor.getComponentConnection
    }
    sourceDeletionResponse <- airbyteClient
      .delete(workspaceId, resource.deepMerge(Json.obj(("workspaceId", Json.fromString(workspaceId)))), resourceType)
  } yield sourceDeletionResponse

  private def unprovisionComponent(componentDescriptor: ComponentDescriptor): Either[Product, String] = for {
    unprovisionedConnectionResponse <- unprovisionResource(componentDescriptor, CONNECTION)
    _                               <- unprovisionResource(componentDescriptor, DESTINATION)
    _                               <- unprovisionResource(componentDescriptor, SOURCE)
  } yield unprovisionedConnectionResponse

  private def runTask(
      descriptor: String,
      compApplication: ComponentDescriptor => Either[Product, String]
  ): Either[Product, String] = for {
    dpHeaderAndComponent <- ComponentExtractor.extract(descriptor)
    componentDescriptor  <- ComponentDescriptor(dpHeaderAndComponent._1, dpHeaderAndComponent._2)
    responses            <- compApplication(componentDescriptor)
  } yield responses

  def provision(descriptorKind: DescriptorKind, descriptor: String): Either[Product, String] = descriptorKind match {
    case COMPONENT_DESCRIPTOR =>
      logger.info("Invoking method {}", "provisionComponent")
      runTask(descriptor, provisionComponent)
    case _ => Left(ValidationError(Seq("Descriptor kind must be COMPONENT_DESCRIPTOR for /provision API")))
  }

  def unprovision(descriptorKind: DescriptorKind, descriptor: String): Either[Product, String] = descriptorKind match {
    case COMPONENT_DESCRIPTOR =>
      logger.info("Invoking method {}", "unprovisionComponent")
      runTask(descriptor, unprovisionComponent)
    case _ => Left(ValidationError(Seq("Descriptor kind must be COMPONENT_DESCRIPTOR for /unprovision API")))
  }

}
