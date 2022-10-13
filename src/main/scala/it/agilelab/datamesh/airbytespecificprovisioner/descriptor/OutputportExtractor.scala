package it.agilelab.datamesh.airbytespecificprovisioner.descriptor

import cats.implicits._
import io.circe.yaml.{parser => yamlParser}
import io.circe.{HCursor, Json}
import it.agilelab.datamesh.airbytespecificprovisioner.model.ValidationError

object OutputportExtractor {

  private val SPECIFIC      = "specific"
  private val COMPONENTS    = "components"
  private val WORKLOADS     = "workloads"
  private val OBSERVABILITY = "observability"
  private val OUTPUT_PORT   = "outputport"
  private val KIND          = "kind"

  def extract(yaml: String): Either[ValidationError, (Json, List[Json])] = yamlParser.parse(yaml) match {
    case Right(json) =>
      val hcursor          = json.hcursor
      val maybeOutputPorts = for {
        dpHeader   <- getDpHeaderDescriptor(hcursor)
        components <- getComponentDescriptors(hcursor)
      } yield (dpHeader, components.filter(isOutputPort))
      maybeOutputPorts match {
        case None      =>
          Left(ValidationError(List(s"""Input data product descriptor is invalid: "components" section not found""")))
        case Some(ops) => Right(ops)
      }
    case Left(_)     => Left(ValidationError(List("Input data product descriptor cannot be parsed")))
  }

  protected def getDpHeaderDescriptor(hcursor: HCursor): Option[Json] = hcursor.keys.fold(None: Option[Json]) { keys =>
    val notAllowedKeys                              = Set(SPECIFIC, COMPONENTS, WORKLOADS, OBSERVABILITY)
    val filteredKeys: Seq[String]                   = keys.toList.filter(key => !notAllowedKeys.contains(key))
    val dpHeaderFields: Option[Seq[(String, Json)]] = filteredKeys.map(key => hcursor.downField(key).focus).sequence
      .map(filteredKeys.zip)
    dpHeaderFields.map(Json.fromFields)
  }

  protected def getComponentDescriptors(hcursor: HCursor): Option[List[Json]] = {
    val componentsHCursor = hcursor.downField(COMPONENTS)
    componentsHCursor.values.map(_.toList)
  }

  protected def isOutputPort(component: Json): Boolean = component.hcursor.downField(KIND).focus
    .exists(_.toString.toLowerCase === s"\"$OUTPUT_PORT\"")

}
