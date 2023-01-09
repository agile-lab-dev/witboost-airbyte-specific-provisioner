package it.agilelab.datamesh.airbytespecificprovisioner.integrator

import com.typesafe.scalalogging.StrictLogging
import io.circe.syntax.EncoderOps
import io.circe.{parser, Json}
import it.agilelab.datamesh.airbytespecificprovisioner.common.Constants.{CONNECTION, DESTINATION, OPERATION, SOURCE}
import it.agilelab.datamesh.airbytespecificprovisioner.descriptor.{ComponentDescriptor, ComponentExtractor}
import it.agilelab.datamesh.airbytespecificprovisioner.model.{
  COMPONENT_DESCRIPTOR,
  DescriptorKind,
  SystemError,
  ValidationError
}
import it.agilelab.datamesh.airbytespecificprovisioner.system.ApplicationConfiguration

class AirbyteWorkloadManager(airbyteClient: AirbyteClient) extends StrictLogging {

  private val workspaceId: String = ApplicationConfiguration.airbyteConfiguration.workspaceId

  private def getIdFromCreationResponse(creationResponse: String, field: String): Either[Product, String] = {
    val creationResponseJson = parser.parse(creationResponse).getOrElse(Json.Null)
    creationResponseJson.hcursor.downField(field).as[String].left
      .map(_ => SystemError(s"Failed to get $field from response"))
  }

  private def provisionSource(componentDescriptor: ComponentDescriptor): Either[Product, String] = for {
    source                 <- componentDescriptor.getComponentSource
    sourceInfo             <- getSourceInfo(source)
    sourceCreationResponse <- airbyteClient.createOrRecreate(workspaceId, sourceInfo, SOURCE)
  } yield sourceCreationResponse

  private def getSourceInfo(source: Json): Either[Product, Json] =
    source.hcursor.downField("connectionConfiguration").as[Json] match {
      case Right(connectionConfig) => Right(source.deepMerge(Json.obj(
          ("workspaceId", Json.fromString(workspaceId)),
          ("sourceDefinitionId", Json.fromString(ApplicationConfiguration.airbyteConfiguration.sourceId)),
          (
            "connectionConfiguration",
            connectionConfig.deepMerge(Json.obj((
              "reader_options",
              Json.fromString(
                "{\"keep_default_na\": false, \"na_values\": [\"-1.#IND\", \"1.#QNAN\", \"1.#IND\", \"-1.#QNAN\", \"#N/A N/A\", \"#N/A\", \"N/A\", \"n/a\", \"\", \"#NA\", \"NULL\", \"null\", \"NaN\", \"-NaN\", \"nan\", \"-nan\"]}"
              )
            )))
          )
        )))
      case _                       => Left(SystemError("Unable to process source connection config"))
    }

  private def getConnectionConfiguration(componentDestination: Json): Either[Product, Json] = componentDestination
    .hcursor.downField("connectionConfiguration").as[Json].left
    .map(_ => SystemError(s"Failed to get connectionConfiguration from destination"))

  private def getDatabase(
      componentDescriptor: ComponentDescriptor,
      connectionConfiguration: Json
  ): Either[Product, String] = connectionConfiguration.hcursor.downField("database").as[String] match {
    case Right(value) if value.nonEmpty => Right(value)
    case _                              => componentDescriptor.getDataProductDomain
  }

  private def getDatabaseSchema(
      componentDescriptor: ComponentDescriptor,
      connectionConfiguration: Json
  ): Either[Product, String] = for {
    dpName    <- componentDescriptor.getDataProductName
    dpVersion <- componentDescriptor.getDataProductVersion
    schema    <- connectionConfiguration.hcursor.downField("schema").as[String] match {
      case Right(value) if value.nonEmpty => Right(value)
      case _ => Right(s"${dpName.toUpperCase.replaceAll(" ", "")}_${dpVersion.split('.')(0)}")
    }
  } yield schema

  private def getDestinationInfo(
      componentDestination: Json,
      connectionConfiguration: Json,
      database: String,
      schema: String
  ): Either[Product, Json] = {
    val password: Json = Json
      .obj(("password", Json.fromString(ApplicationConfiguration.snowflakeConfiguration.password)))

    val method: Json = Json.obj(("method", Json.fromString("Internal Staging")))

    Right(componentDestination.deepMerge(Json.obj((
      "connectionConfiguration",
      connectionConfiguration.deepMerge(Json.obj(
        ("host", Json.fromString(ApplicationConfiguration.snowflakeConfiguration.host)),
        ("role", Json.fromString(ApplicationConfiguration.snowflakeConfiguration.role)),
        ("username", Json.fromString(ApplicationConfiguration.snowflakeConfiguration.user)),
        ("warehouse", Json.fromString(ApplicationConfiguration.snowflakeConfiguration.warehouse)),
        ("database", Json.fromString(database.toUpperCase())),
        ("schema", Json.fromString(schema)),
        ("credentials", password),
        ("loading_method", method)
      ))
    ))))
  }

  private def provisionDestination(componentDescriptor: ComponentDescriptor): Either[Product, String] = for {
    destination                 <- componentDescriptor.getComponentDestination
    connectionConfiguration     <- getConnectionConfiguration(destination)
    db                          <- getDatabase(componentDescriptor, connectionConfiguration)
    dbSchema                    <- getDatabaseSchema(componentDescriptor, connectionConfiguration)
    destinationInfo             <- getDestinationInfo(destination, connectionConfiguration, db, dbSchema)
    destinationCreationResponse <- airbyteClient.createOrRecreate(
      workspaceId,
      destinationInfo.deepMerge(Json.obj(
        ("workspaceId", Json.fromString(workspaceId)),
        ("destinationDefinitionId", Json.fromString(ApplicationConfiguration.airbyteConfiguration.destinationId))
      )),
      DESTINATION
    )
  } yield destinationCreationResponse

  private def provisionOperation(): Either[Product, String] = for {
    operationCreationResponse <- airbyteClient.createOrRecreate(
      workspaceId,
      Json.obj(
        ("workspaceId", Json.fromString(workspaceId)),
        ("name", Json.fromString("Basic normalization")),
        (
          "operatorConfiguration",
          Json.obj(
            ("operatorType", Json.fromString("normalization")),
            ("normalization", Json.obj(("option", Json.fromString("basic"))))
          )
        )
      ),
      OPERATION
    )
  } yield operationCreationResponse

  private def provisionDbtOperation(componentDescriptor: ComponentDescriptor): Either[Product, String] =
    componentDescriptor.getDbtGitUrl match {
      case gitRepoUrl if gitRepoUrl.nonEmpty =>
        airbyteClient.createOrRecreate(
          workspaceId,
          Json.obj(
            ("workspaceId", Json.fromString(workspaceId)),
            ("name", Json.fromString("Dbt transformation")),
            (
              "operatorConfiguration",
              Json.obj(
                ("operatorType", Json.fromString("dbt")),
                (
                  "dbt",
                  Json.obj(
                    ("gitRepoUrl", Json.fromString(gitRepoUrl)),
                    ("gitRepoBranch", Json.fromString("")),
                    ("dockerImage", Json.fromString("fishtownanalytics/dbt:1.0.0")),
                    ("dbtArguments", Json.fromString("run"))
                  )
                )
              )
            )
          ),
          OPERATION
        )

      case _ =>
        logger.info("Skipping Dbt Airbyte operation creation")
        Right("")
    }

  private def provisionConnection(
      componentDescriptor: ComponentDescriptor,
      connectionInfo: Json,
      provisionedSourceId: String,
      provisionedDestinationId: String,
      provisionedOperationIds: List[String]
  ): Either[Product, String] = for {
    connectionName              <- componentDescriptor.getConnectionName
    connectionCreationResponses <- airbyteClient.createOrRecreate(
      workspaceId,
      Json.obj(
        ("syncCatalog", connectionInfo),
        ("sourceId", Json.fromString(provisionedSourceId)),
        ("destinationId", Json.fromString(provisionedDestinationId)),
        ("operationIds", provisionedOperationIds.asJson),
        ("scheduleType", Json.fromString("manual")),
        ("status", Json.fromString("active")),
        ("name", Json.fromString(connectionName))
      ),
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
    provisionedOperation           <- provisionOperation()
    provisionedDbtOperation        <- provisionDbtOperation(componentDescriptor)
    provisionedSourceId            <- getIdFromCreationResponse(provisionedSourceResponse, "sourceId")
    provisionedDestinationId       <- getIdFromCreationResponse(provisionedDestinationResponse, "destinationId")
    provisionedOperationId         <- getIdFromCreationResponse(provisionedOperation, "operationId")
    discoveredSchemaResponse       <- airbyteClient.discoverSchema(provisionedSourceId)
    connectionInfo                 <- getConnectionInfo(discoveredSchemaResponse)
    provisionedConnection          <- provisionedDbtOperation match {
      case dbtOperation if dbtOperation.nonEmpty =>
        getIdFromCreationResponse(provisionedDbtOperation, "operationId") match {
          case Left(value)                      => Left(value)
          case Right(provisionedDbtOperationId) => provisionConnection(
              componentDescriptor,
              connectionInfo,
              provisionedSourceId,
              provisionedDestinationId,
              List(provisionedOperationId, provisionedDbtOperationId)
            )
        }
      case _                                     => provisionConnection(
          componentDescriptor,
          connectionInfo,
          provisionedSourceId,
          provisionedDestinationId,
          List(provisionedOperationId)
        )
    }
  } yield provisionedConnection

  private def unprovisionResource(
      componentDescriptor: ComponentDescriptor,
      resourceType: String
  ): Either[Product, String] = for {
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
