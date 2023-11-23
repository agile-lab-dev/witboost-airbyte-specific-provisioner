package it.agilelab.datamesh.airbytespecificprovisioner.integrator

import cats.data.ValidatedNel
import cats.implicits._
import com.typesafe.scalalogging.StrictLogging
import io.circe.syntax.EncoderOps
import io.circe.{parser, Json}
import it.agilelab.datamesh.airbytespecificprovisioner.common.Constants.{CONNECTION, DESTINATION, OPERATION, SOURCE}
import it.agilelab.datamesh.airbytespecificprovisioner.error.{
  ErrorType,
  GetConnectionInfoErrorType,
  GetIdFromCreationErrorType,
  ValidationErrorType
}
import it.agilelab.datamesh.airbytespecificprovisioner.model.{AirbyteFields, DescriptorKind}
import it.agilelab.datamesh.airbytespecificprovisioner.system.ApplicationConfiguration
import it.agilelab.datamesh.airbytespecificprovisioner.validation.Validator

class AirbyteWorkloadManager(validator: Validator, airbyteClient: Client) extends StrictLogging {

  private val workspaceId: String = ApplicationConfiguration.airbyteConfiguration.workspaceId

  def validate(descriptorKind: DescriptorKind, descriptor: String): ValidatedNel[ValidationErrorType, AirbyteFields] =
    validator.validate(descriptorKind, descriptor)

  def provision(descriptorKind: DescriptorKind, descriptor: String): ValidatedNel[ErrorType, String] = validator
    .validate(descriptorKind, descriptor).andThen(airbyteFields =>
      provisionSource(airbyteFields).andThen(sourceResponse =>
        provisionDestination(airbyteFields).andThen(destinationResponse =>
          provisionOperation().andThen(operationResponse =>
            getIdFromCreationResponse(sourceResponse, "sourceId").andThen(provisionedSourceId =>
              getIdFromCreationResponse(destinationResponse, "destinationId").andThen(provisionedDestinationId =>
                getIdFromCreationResponse(operationResponse, "operationId").andThen(provisionedOperationId =>
                  airbyteClient.discoverSchema(provisionedSourceId).toValidatedNel.andThen(discoveredSchemaResponse =>
                    getConnectionInfo(discoveredSchemaResponse).andThen(connectionInfo =>
                      provisionConnection(
                        airbyteFields.connection.name,
                        connectionInfo,
                        provisionedSourceId,
                        provisionedDestinationId,
                        List(provisionedOperationId)
                      )
                    )
                  )
                )
              )
            )
          )
        )
      )
    )

  def unprovision(descriptorKind: DescriptorKind, descriptor: String): ValidatedNel[ErrorType, Unit] = validator
    .validate(descriptorKind, descriptor).andThen(airbyteFields =>
      unprovisionResource(airbyteFields, CONNECTION).andThen(_ =>
        (unprovisionResource(airbyteFields, DESTINATION), unprovisionResource(airbyteFields, SOURCE)).mapN((_, _) => ())
      )
    )

  private def provisionSource(airbyteFields: AirbyteFields) = {
    val finalSource = airbyteFields.source.raw.deepMerge(Json.obj(
      ("workspaceId", Json.fromString(workspaceId)),
      ("sourceDefinitionId", Json.fromString(ApplicationConfiguration.airbyteConfiguration.sourceId)),
      (
        "connectionConfiguration",
        airbyteFields.source.connectionConfiguration.deepMerge(Json.obj((
          "reader_options",
          Json.fromString(
            "{\"keep_default_na\": false, \"na_values\": [\"-1.#IND\", \"1.#QNAN\", \"1.#IND\", \"-1.#QNAN\", \"#N/A N/A\", \"#N/A\", \"N/A\", \"n/a\", \"\", \"#NA\", \"NULL\", \"null\", \"NaN\", \"-NaN\", \"nan\", \"-nan\"]}"
          )
        )))
      )
    ))
    airbyteClient.deleteAndRecreate(workspaceId, finalSource, airbyteFields.source.name, SOURCE).toValidatedNel
  }

  private def provisionDestination(airbyteFields: AirbyteFields) = {
    val database        = getDatabase(airbyteFields)
    val schema          = getDatabaseSchema(airbyteFields)
    val destinationInfo = getDestinationInfo(
      airbyteFields.destination.raw,
      airbyteFields.destination.connectionConfiguration,
      database,
      schema
    )
    airbyteClient.deleteAndRecreate(
      workspaceId,
      destinationInfo.deepMerge(Json.obj(
        ("workspaceId", Json.fromString(workspaceId)),
        ("destinationDefinitionId", Json.fromString(ApplicationConfiguration.airbyteConfiguration.destinationId))
      )),
      airbyteFields.destination.name,
      DESTINATION
    ).toValidatedNel
  }

  private def provisionOperation() = airbyteClient.create(
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
  ).toValidatedNel

  private def getIdFromCreationResponse(creationResponse: String, field: String) = parser.parse(creationResponse)
    .leftMap { e =>
      logger.error(s"Failed to parse creation response", e)
      GetIdFromCreationErrorType(s"Failed to parse creation response. Details: ${e.getMessage}")
    }.toValidatedNel.andThen { creationResponseJson =>
      creationResponseJson.hcursor.downField(field).as[String].leftMap { e =>
        logger.error(s"Failed to get $field from response", e)
        GetIdFromCreationErrorType(s"Failed to get $field from response")
      }.toValidatedNel
    }

  private def getConnectionInfo(schemaResponse: String) = {
    val schemaResponseJson = parser.parse(schemaResponse).getOrElse(Json.Null)
    val info               = schemaResponseJson.hcursor.downField("catalog").as[Json] match {
      case Left(e)        =>
        logger.error("Failed to get catalog from response", e)
        Left(GetConnectionInfoErrorType(s"Failed to get catalog from response"))
      case Right(catalog) => catalog.hcursor.downField("streams").downArray.as[Json] match {
          case Left(e)            =>
            logger.error("Failed to get streams from response", e)
            Left(GetConnectionInfoErrorType(s"Failed to get streams from response"))
          case Right(streamsJson) =>
            val filteredStreams: Option[Json] = streamsJson.hcursor.downField("stream").downField("jsonSchema")
              .withFocus(_.asObject.map(_.filterKeys(!_.equals("$schema")).asJson).get).top
            filteredStreams match {
              case Some(value) =>
                val updatedStreams = value.hcursor.downField("config").downField("destinationSyncMode")
                  .withFocus(_.mapString(_ => "overwrite")).top
                updatedStreams match {
                  case Some(value) => Right(catalog.deepMerge(Json.obj(("streams", Json.arr(value)))))
                  case _           =>
                    logger.error("Unable to process stream config")
                    Left(GetConnectionInfoErrorType("Unable to process stream config"))
                }
              case _           =>
                logger.error("Unable to process stream jsonSchema")
                Left(GetConnectionInfoErrorType("Unable to process stream jsonSchema"))
            }
        }
    }
    info.toValidatedNel
  }

  private def provisionConnection(
      connectionName: String,
      connectionInfo: Json,
      provisionedSourceId: String,
      provisionedDestinationId: String,
      provisionedOperationIds: List[String]
  ) = airbyteClient.deleteAndRecreate(
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
    connectionName,
    CONNECTION
  ).toValidatedNel

  private def getDatabase(airbyteFields: AirbyteFields) =
    airbyteFields.destination.connectionConfiguration.hcursor.downField("database").as[String] match {
      case Right(value) if value.nonEmpty => value
      case _                              => airbyteFields.dpFields.domain
    }

  private def getDatabaseSchema(airbyteFields: AirbyteFields) =
    airbyteFields.destination.connectionConfiguration.hcursor.downField("schema").as[String] match {
      case Right(value) if value.nonEmpty => value
      case _                              =>
        s"${airbyteFields.dpFields.name.toUpperCase.replaceAll(" ", "")}_${airbyteFields.dpFields.version.split('.')(0)}"
    }

  private def getDestinationInfo(
      componentDestination: Json,
      connectionConfiguration: Json,
      database: String,
      schema: String
  ) = {
    val password: Json = Json
      .obj(("password", Json.fromString(ApplicationConfiguration.snowflakeConfiguration.password)))
    val method: Json   = Json.obj(("method", Json.fromString("Internal Staging")))
    componentDestination.deepMerge(Json.obj((
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
    )))
  }

  private def unprovisionResource(airbyteFields: AirbyteFields, resourceType: String) = {
    val resourceName = resourceType match {
      case SOURCE      => airbyteFields.source.name
      case DESTINATION => airbyteFields.destination.name
      case CONNECTION  => airbyteFields.connection.name
    }
    airbyteClient.delete(workspaceId, resourceName, resourceType).toValidatedNel
  }

}
