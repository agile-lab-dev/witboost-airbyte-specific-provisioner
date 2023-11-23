package it.agilelab.datamesh.airbytespecificprovisioner.descriptor

import cats.implicits.{catsSyntaxEq, toTraverseOps}
import com.typesafe.scalalogging.StrictLogging
import io.circe.yaml.syntax.AsYaml
import io.circe.{HCursor, Json}
import it.agilelab.datamesh.airbytespecificprovisioner.common.Constants._
import it.agilelab.datamesh.airbytespecificprovisioner.error.{InvalidComponent, ValidationErrorType}

final case class ComponentDescriptor(
    dpId: String,
    dpHeader: Json,
    compId: String,
    compKind: String,
    compHeader: Json,
    compSpecific: Json
) extends StrictLogging {

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

  def apply(dpHeader: Json, component: Json): Either[ValidationErrorType, ComponentDescriptor] = {
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
      case _ => Left(InvalidComponent(compId.getOrElse("Unknown")))
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
