package it.agilelab.datamesh.airbytespecificprovisioner.integrator

import com.typesafe.scalalogging.StrictLogging
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
      provisionedSourceId: String,
      provisionedDestinationId: String
  ): Either[Product, String] = for {
    workspaceId                 <- componentDescriptor.getComponentWorkspaceId
    componentConnection         <- componentDescriptor.getComponentConnection
    connectionCreationResponses <- airbyteClient.createOrRecreate(
      workspaceId,
      componentConnection.deepMerge(Json.obj(("sourceId", Json.fromString(provisionedSourceId))))
        .deepMerge(Json.obj(("destinationId", Json.fromString(provisionedDestinationId)))),
      CONNECTION
    )
  } yield connectionCreationResponses

  private def provisionComponent(componentDescriptor: ComponentDescriptor): Either[Product, String] = for {
    provisionedSourceResponse      <- provisionSource(componentDescriptor)
    provisionedDestinationResponse <- provisionDestination(componentDescriptor)
    provisionedSourceId            <- getIdFromCreationResponse(provisionedSourceResponse, "sourceId")
    provisionedDestinationId       <- getIdFromCreationResponse(provisionedDestinationResponse, "destinationId")
    provisionedConnections <- provisionConnection(componentDescriptor, provisionedSourceId, provisionedDestinationId)
  } yield provisionedConnections

  private def runTask(
      descriptor: String,
      compApplication: ComponentDescriptor => Either[Product, String]
  ): Either[Product, String] = for {
    dpHeaderAndComponent <- ComponentExtractor.extract(descriptor)
    componentDescriptor  <- ComponentDescriptor(dpHeaderAndComponent._1, dpHeaderAndComponent._2)
    responses            <- compApplication(componentDescriptor)
  } yield responses

  def validate(descriptorKind: DescriptorKind, descriptor: String): Either[Product, Boolean] = ???

  def provision(descriptorKind: DescriptorKind, descriptor: String): Either[Product, String] = descriptorKind match {
    case COMPONENT_DESCRIPTOR =>
      logger.info("Invoking method {}", "provisionComponent")
      runTask(descriptor, provisionComponent)
    case _ => Left(ValidationError(Seq("Descriptor kind must be COMPONENT_DESCRIPTOR for /provision API")))
  }

}
