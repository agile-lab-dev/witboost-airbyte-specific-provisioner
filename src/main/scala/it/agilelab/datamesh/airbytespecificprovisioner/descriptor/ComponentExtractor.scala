package it.agilelab.datamesh.airbytespecificprovisioner.descriptor

import cats.implicits._
import io.circe.yaml.{parser => yamlParser}
import io.circe.{HCursor, Json}
import it.agilelab.datamesh.airbytespecificprovisioner.common.Constants.{ID, WORKLOAD}
import it.agilelab.datamesh.airbytespecificprovisioner.error.{
  InvalidDescriptor,
  ParseFailureDescriptor,
  ValidationErrorType
}

object ComponentExtractor {

  private val DATA_PRODUCT_FIELD        = "dataProduct"
  private val SPECIFIC                  = "specific"
  private val COMPONENTS                = "components"
  private val COMPONENT_ID_TO_PROVISION = "componentIdToProvision"
  private val WORKLOADS                 = "workloads"
  private val OBSERVABILITY             = "observability"
  private val KIND                      = "kind"

  def extract(yaml: String): Either[ValidationErrorType, (Json, Json)] = yamlParser.parse(yaml) match {
    case Right(json) =>
      val hcursor                   = json.hcursor
      val maybeComponentToProvision = for {
        dpHeader               <- getDpHeader(hcursor)
        componentIdToProvision <- getComponentIdToProvision(hcursor)
        componentToProvision   <- getComponent(hcursor, componentIdToProvision)
      } yield (dpHeader, componentToProvision)
      maybeComponentToProvision match {
        case None      => Left(InvalidDescriptor())
        case Some(ops) => Right(ops)
      }
    case Left(e)     => Left(ParseFailureDescriptor(e))
  }

  protected def getDpHeader(hcursor: HCursor): Option[Json] = hcursor.downField(DATA_PRODUCT_FIELD).keys
    .fold(None: Option[Json]) { keys =>
      val notAllowedKeys                              = Set(SPECIFIC, COMPONENTS, WORKLOADS, OBSERVABILITY)
      val filteredKeys: Seq[String]                   = keys.toList.filter(key => !notAllowedKeys.contains(key))
      val dpHeaderFields: Option[Seq[(String, Json)]] = filteredKeys
        .map(key => hcursor.downField(DATA_PRODUCT_FIELD).downField(key).focus).sequence.map(filteredKeys.zip)
      dpHeaderFields.map(Json.fromFields)
    }

  protected def getComponent(hcursor: HCursor, componentIdToProvision: String): Option[Json] = {
    val componentsHCursor = hcursor.downField(DATA_PRODUCT_FIELD).downField(COMPONENTS)
    componentsHCursor.values.map(_.toList).map(components =>
      components.filter(comp => isComponentToProvision(comp.hcursor, componentIdToProvision))
    ) match {
      case Some(componentToProvision :: Nil) => Some(componentToProvision)
      case _                                 => None
    }
  }

  protected def getComponentIdToProvision(hcursor: HCursor): Option[String] = hcursor
    .downField(COMPONENT_ID_TO_PROVISION).as[String].toOption

  protected def isComponentToProvision(hcursor: HCursor, componentIdToProvision: String): Boolean = hcursor
    .downField(ID).as[String].toOption.getOrElse("").equals(componentIdToProvision)

  protected def isWorkload(component: Json): Boolean = component.hcursor.downField(KIND).focus
    .exists(_.toString.toLowerCase === s"\"$WORKLOAD\"")

}
