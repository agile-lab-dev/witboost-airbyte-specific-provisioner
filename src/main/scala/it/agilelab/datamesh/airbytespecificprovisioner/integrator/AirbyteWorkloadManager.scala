package it.agilelab.datamesh.airbytespecificprovisioner.integrator

import cats.implicits._
import com.typesafe.scalalogging.StrictLogging
import io.circe.{parser, Json}
import it.agilelab.datamesh.airbytespecificprovisioner.descriptor.{ComponentDescriptor, ComponentExtractor}
import it.agilelab.datamesh.airbytespecificprovisioner.model.{
  COMPONENT_DESCRIPTOR,
  DescriptorKind,
  SystemError,
  ValidationError
}

class AirbyteWorkloadManager(airbyteClient: AirbyteClient) extends StrictLogging {

  private def mapConnectionSourceIdDestinationId(
      connectionJson: Json,
      provisionedSources: Map[String, String],
      provisionedDestinations: Map[String, String]
  ): Either[SystemError, Json] = {
    val connectionName = connectionJson.hcursor.downField("name").as[String].getOrElse("")
    connectionJson.hcursor.downField("sourceId").withFocus(_.mapString(s => provisionedSources(s))).up
      .downField("destinationId").withFocus(_.mapString(s => provisionedDestinations(s))).top
      .toRight(SystemError(s"Failed to map sourceId or destinationId in connection $connectionName"))
  }

  private def getIdFromCreationResponse(
      key: String,
      creationResponse: String,
      field: String
  ): Either[Product, (String, String)] = {
    val creationResponseJson = parser.parse(creationResponse).getOrElse(Json.Null)
    creationResponseJson.hcursor.downField(field).as[String].toOption.map(id => (key, id))
      .toRight(SystemError(s"Failed to get $field from response"))
  }

  private def provisionSources(componentDescriptor: ComponentDescriptor): Either[Product, Map[String, String]] = for {
    workspaceId             <- componentDescriptor.getComponentWorkspaceId
    sourceKeys              <- componentDescriptor.getComponentSourceKeys
    sources                 <- sourceKeys.map(sourceKey =>
      componentDescriptor.getSourceByKey(sourceKey)
        .map(j => (sourceKey, j.deepMerge(Json.obj(("workspaceId", Json.fromString(workspaceId))))))
    ).sequence.map(_.toMap)
    sourceCreationResponses <- sources
      .map(source => airbyteClient.createSource(source._2.toString).map(response => (source._1, response))).toList
      .sequence.map(_.toMap)
  } yield sourceCreationResponses

  private def provisionDestinations(componentDescriptor: ComponentDescriptor): Either[Product, Map[String, String]] =
    for {
      workspaceId                  <- componentDescriptor.getComponentWorkspaceId
      destinationKeys              <- componentDescriptor.getComponentDestinationKeys
      destinations                 <- destinationKeys.map(destinationKey =>
        componentDescriptor.getDestinationByKey(destinationKey)
          .map(j => (destinationKey, j.deepMerge(Json.obj(("workspaceId", Json.fromString(workspaceId))))))
      ).sequence.map(_.toMap)
      destinationCreationResponses <- destinations.map(destination =>
        airbyteClient.createDestination(destination._2.toString).map(response => (destination._1, response))
      ).toList.sequence.map(_.toMap)
    } yield destinationCreationResponses

  private def provisionConnections(
      componentDescriptor: ComponentDescriptor,
      provisionedSources: Map[String, String],
      provisionedDestinations: Map[String, String]
  ): Either[Product, List[String]] = for {
    componentConnections        <- componentDescriptor.getComponentConnections
    mappedComponentConnections  <- componentConnections.map(connectionJson =>
      mapConnectionSourceIdDestinationId(connectionJson, provisionedSources, provisionedDestinations)
    ).sequence
    connectionCreationResponses <- mappedComponentConnections
      .map(connection => airbyteClient.createConnection(connection.toString)).sequence
  } yield connectionCreationResponses

  private def provisionComponent(componentDescriptor: ComponentDescriptor): Either[Product, List[String]] = for {
    provisionedSourceResponses      <- provisionSources(componentDescriptor)
    provisionedDestinationResponses <- provisionDestinations(componentDescriptor)
    provisionedSourceMapping        <- provisionedSourceResponses
      .map(kv => getIdFromCreationResponse(kv._1, kv._2, "sourceId")).toList.sequence.map(_.toMap)
    provisionedDestinationMapping   <- provisionedDestinationResponses
      .map(kv => getIdFromCreationResponse(kv._1, kv._2, "destinationId")).toList.sequence.map(_.toMap)
    provisionedConnections          <-
      provisionConnections(componentDescriptor, provisionedSourceMapping, provisionedDestinationMapping)
  } yield provisionedConnections

  private def runTask(
      descriptor: String,
      compApplication: ComponentDescriptor => Either[Product, List[String]]
  ): Either[Product, List[String]] = for {
    dpHeaderAndComponent <- ComponentExtractor.extract(descriptor)
    componentDescriptor  <- ComponentDescriptor(dpHeaderAndComponent._1, dpHeaderAndComponent._2)
    responses            <- compApplication(componentDescriptor)
  } yield responses

  def validate(descriptorKind: DescriptorKind, descriptor: String): Either[Product, Boolean] = ???

  def provision(descriptorKind: DescriptorKind, descriptor: String): Either[Product, String] = descriptorKind match {
    case COMPONENT_DESCRIPTOR =>
      logger.info("Invoking method {}", "provisionComponent")
      runTask(descriptor, provisionComponent).map(_.mkString(";"))
    case _ => Left(ValidationError(Seq("Descriptor kind must be COMPONENT_DESCRIPTOR for /provision API")))
  }

}
