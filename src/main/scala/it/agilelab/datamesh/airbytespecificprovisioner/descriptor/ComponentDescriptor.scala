package it.agilelab.datamesh.airbytespecificprovisioner.descriptor

import cats.implicits.{catsSyntaxEq, toTraverseOps}
import com.typesafe.scalalogging.StrictLogging
import io.circe.yaml.syntax.AsYaml
import io.circe.{HCursor, Json}
import it.agilelab.datamesh.airbytespecificprovisioner.common.Constants._
import it.agilelab.datamesh.airbytespecificprovisioner.model.{SystemError, ValidationError}
import it.agilelab.datamesh.airbytespecificprovisioner.system.ApplicationConfiguration

final case class ComponentDescriptor(
    dpId: String,
    dpHeader: Json,
    compId: String,
    compKind: String,
    compHeader: Json,
    compSpecific: Json
) extends StrictLogging {

  private val accessToken: String = ApplicationConfiguration.airbyteConfiguration.dbtGitToken
  private val userName: String    = ApplicationConfiguration.airbyteConfiguration.dbtGitUser

  // ==== INFO FROM DATA PRODUCT HEADER =====================================================================

  def getDataProductId: Either[ValidationError, String] = dpHeader.hcursor.downField(ID).as[String].left
    .map(_ => ValidationError(Seq(s"Mandatory dataproduct parameter $ID is missing")))

  def getDataProductName: Either[ValidationError, String] = dpHeader.hcursor.downField(NAME).as[String].left
    .map(_ => ValidationError(Seq(s"Mandatory dataproduct parameter $NAME is missing")))

  def getDataProductDomain: Either[ValidationError, String] = dpHeader.hcursor.downField(DOMAIN).as[String].left
    .map(_ => ValidationError(Seq(s"Mandatory dataproduct parameter $DOMAIN is missing")))

  def getDataProductVersion: Either[ValidationError, String] = dpHeader.hcursor.downField(VERSION).as[String].left
    .map(_ => ValidationError(Seq(s"Mandatory dataproduct parameter $VERSION is missing")))

  def getCollectionName: Either[ValidationError, String] = for {
    domainName <- getDataProductDomain
    dpName     <- getDataProductName
  } yield Seq(domainName, dpName).map(_.replaceAll(NOT_ALPHANUMERIC_NOR_DASH, EMPTY_STRING)).mkString(DASH)

  def getEnvironment: Either[ValidationError, String] = dpHeader.hcursor.downField(ENVIRONMENT).as[String].left
    .map(_ => ValidationError(Seq(s"Mandatory dataproduct parameter $ENVIRONMENT is missing")))

  def getDataProductOwner: Either[ValidationError, String] = dpHeader.hcursor.downField(DATA_PRODUCT_OWNER).as[String]
    .left.map(_ => ValidationError(Seq(s"Mandatory data product header parameter $DATA_PRODUCT_OWNER is missing")))
    .map(_.replaceAll(PREFIX_WITH_COLON, EMPTY_STRING).replaceAll(UNDERSCORE, AT).trim)

  def getOwnerGroup: Either[ValidationError, String] = dpHeader.hcursor.downField(OWNER_GROUP).as[String].left
    .map(_ => ValidationError(Seq(s"Mandatory data product header parameter $OWNER_GROUP is missing")))
    .map(_.replaceAll(PREFIX_WITH_COLON, EMPTY_STRING).replaceAll(UNDERSCORE, AT).trim)

  def getEmail: Either[ValidationError, String] = dpHeader.hcursor.downField(EMAIL).as[String].left
    .map(_ => ValidationError(Seq(s"Optional data product header parameter $EMAIL is missing")))

  def getContactsEmails: Either[ValidationError, List[String]] = List(getDataProductOwner, getOwnerGroup, getEmail)
    .filter(_.isRight).distinct.sequence

  // ==== INFO FROM OUTPUT PORT HEADER =========================================================================
  def getComponentId: Either[ValidationError, String] = compHeader.hcursor.downField(ID).as[String].left
    .map(_ => ValidationError(Seq(s"Mandatory workload parameter $ID is missing")))

  def getComponentName: Either[ValidationError, String] = compHeader.hcursor.downField(NAME).as[String].left
    .map(_ => ValidationError(Seq(s"Mandatory parameter $NAME is missing in workload ${getComponentId.getOrElse("")}")))

  def getComponentDescription: Either[ValidationError, String] = compHeader.hcursor.downField(DESCRIPTION).as[String]
    .left.map(_ =>
      ValidationError(Seq(s"Mandatory parameter $DESCRIPTION is missing in workload ${getComponentId.getOrElse("")}"))
    )

  def getComponentVersion: Either[ValidationError, String] = compHeader.hcursor.downField(VERSION).as[String].left
    .map(_ =>
      ValidationError(Seq(s"Mandatory parameter $VERSION is missing in workload ${getComponentId.getOrElse("")}"))
    )

  def getComponentCreationDate: Option[String] = compHeader.hcursor.downField(CREATION_DATE).as[String].toOption

  def getComponentProcessDescription: Option[String] = compHeader.hcursor.downField(PROCESS_DESCRIPTION).as[String]
    .toOption

  def getComponentDataContract: Either[ValidationError, Json] = compHeader.hcursor.downField(DATA_CONTRACT).as[Json]
    .left.map(_ =>
      ValidationError(Seq(s"Mandatory object $DATA_CONTRACT is missing in workload ${getComponentId.getOrElse("")}"))
    )

  // ==== INFO FROM OUTPUT PORT SPECIFIC ======================================================================
  def getComponentSpecific: Either[ValidationError, Json] = Right(compSpecific)

  def getComponentSource: Either[ValidationError, Json] = compSpecific.hcursor.downField("source").as[Json].left
    .map(_ => ValidationError(Seq("Failed to retrieve component specific source")))

  def getComponentDestination: Either[ValidationError, Json] = compSpecific.hcursor.downField("destination").as[Json]
    .left.map(_ => ValidationError(Seq("Failed to retrieve component specific destination")))

  def getComponentConnection: Either[ValidationError, Json] = compSpecific.hcursor.downField("connection").as[Json].left
    .map(_ => ValidationError(Seq(s"Failed to retrieve component specific connection")))

  def getConnectionName: Either[ValidationError, String] = getComponentConnection match {
    case Right(connection) => connection.hcursor.downField("name").as[String] match {
        case Right(connectionName) => Right(connectionName)
        case _                     => Left(ValidationError(Seq("Failed to retrieve connection name")))
      }
    case Left(error)       => Left(error)
  }

  def getDbtGitUrl: String = getComponentConnection match {
    case Right(connection) => connection.hcursor.downField("dbtGitUrl").as[String] match {
        case Right(url) => convertUrl(url)
        case _          => ""
      }
    case _                 => ""
  }

  private def convertUrl(url: String): String = {
    val splitArray    = url.split("(?<=//)")
    val httpServer    = splitArray(0).patch(8, userName, 0)
    val basePath      = splitArray(1).split("/")(0).patch(0, accessToken + "@", 0)
    val directoryPath = splitArray(1).patch(0, "", 10)
    val finalUrl      = httpServer + ":" + basePath + directoryPath
    finalUrl
  }

  // ==== VALIDATION UTILITIES ================================================================================
  def validateComponent: Either[Product, ComponentDescriptor] = {
    val validationErrors = List(
      getDataProductId,
      getDataProductName,
      getDataProductDomain,
      getDataProductVersion,
      getEnvironment,
      getDataProductOwner,
      getOwnerGroup,
      getComponentId,
      getComponentName,
      getComponentVersion,
      getComponentDataContract
    ).flatMap(_.swap.toOption match {
      case Some(err) => Some(err)
      case _         => None
    })
    validationErrors match {
      case l if l.isEmpty  => Right(this)
      case l if l.nonEmpty =>
        val allErrors = l.flatMap(ve => ve.errors).distinct
        Left(ValidationError(allErrors))
      case _ => Left(SystemError(s"Failed to perform validation of workload ${getComponentId.getOrElse("")}"))
    }
  }

  // ==== OVERRIDING STRING METHOD ================================================================================
  override def toString: String = {
    val dataproduct: Json = Json.obj((DATA_PRODUCT, dpHeader))
    val component: Json   = Json.obj((
      COMPONENT,
      compHeader.deepMerge(Json.obj((SPECIFIC, compSpecific))).deepMerge(Json.obj((PROVISIONING_RESULT, compSpecific)))
    ))
    s"${dataproduct.asYaml.spaces2}${component.asYaml.spaces2}"
  }

}

object ComponentDescriptor {

  def apply(dpHeader: Json, component: Json): Either[ValidationError, ComponentDescriptor] = {
    val (dpId, compId, compKind, compHeader, compSpecific) = (
      getId(dpHeader.hcursor),
      getComponentId(component.hcursor),
      getComponentKind(component.hcursor),
      getComponentHeaderDescriptor(component.hcursor),
      getComponentSpecificDescriptor(component.hcursor)
    )
    (dpId, compId, compKind, compHeader, compSpecific) match {
      case (Some(dataProductId), Some(compId), Some(WORKLOAD), Some(compHeader), Some(compSpecific)) =>
        Right(ComponentDescriptor(dataProductId, dpHeader, compId, WORKLOAD, compHeader, compSpecific))
      case _ => Left(ValidationError(List(s"The workload ${compId.getOrElse("")} descriptor is not valid")))
    }
  }

  def getId(hcursor: HCursor): Option[String] = hcursor.downField(ID).as[String].toOption

  def getComponentId(hcursor: HCursor): Option[String] = hcursor.downField(ID).as[String].toOption

  def getComponentKind(hcursor: HCursor): Option[String] = hcursor.downField(KIND).as[String].toOption

  private def getComponentHeaderDescriptor(hcursor: HCursor): Option[Json] = hcursor.keys
    .fold(None: Option[Json]) { keys =>
      val filteredKeys: Seq[String] = keys.toList.filter(key => !(key === SPECIFIC) && !(key === PROVISIONING_RESULT))
      val dpHeaderFields: Option[Seq[(String, Json)]] = filteredKeys.map(key => hcursor.downField(key).focus)
        .traverse(identity).map(filteredKeys.zip)
      dpHeaderFields.map(Json.fromFields)
    }

  def getComponentSpecificDescriptor(hcursor: HCursor): Option[Json] = hcursor.downField(SPECIFIC).as[Json].toOption

}
