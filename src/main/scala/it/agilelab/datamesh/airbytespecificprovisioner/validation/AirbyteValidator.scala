package it.agilelab.datamesh.airbytespecificprovisioner.validation

import cats.data.ValidatedNel
import com.typesafe.scalalogging.LazyLogging
import it.agilelab.datamesh.airbytespecificprovisioner.descriptor.{ComponentDescriptor, ComponentExtractor}
import it.agilelab.datamesh.airbytespecificprovisioner.error.{
  InvalidDescriptorKind,
  MissingHeaderField,
  MissingSpecificConnectionField,
  MissingSpecificDestinationField,
  MissingSpecificField,
  MissingSpecificSourceField,
  ValidationErrorType
}
import cats.implicits._
import io.circe.Json
import it.agilelab.datamesh.airbytespecificprovisioner.common.Constants.{DOMAIN, NAME, VERSION}
import it.agilelab.datamesh.airbytespecificprovisioner.model.{
  AirbyteConnection,
  AirbyteDestination,
  AirbyteFields,
  AirbyteSource,
  COMPONENT_DESCRIPTOR,
  DataProductFields,
  DescriptorKind
}

class AirbyteValidator extends Validator with LazyLogging {

  override def validate(
      descriptorKind: DescriptorKind,
      descriptor: String
  ): ValidatedNel[ValidationErrorType, AirbyteFields] = validateDescriptorKind(descriptorKind)
    .productR(validateComponentDescriptor(descriptor).andThen(cDescriptor =>
      (
        validateDataProductFields(cDescriptor),
        validateSource(cDescriptor),
        validateDestination(cDescriptor),
        validateConnection(cDescriptor)
      ).mapN((dpFields, aSource, aDest, aConn) => AirbyteFields(dpFields, aSource, aDest, aConn))
    ))

  private def validateComponentDescriptor(descriptor: String): ValidatedNel[ValidationErrorType, ComponentDescriptor] =
    ComponentExtractor.extract(descriptor).toValidatedNel
      .andThen(parts => ComponentDescriptor(parts._1, parts._2).toValidatedNel)

  private def validateDescriptorKind(descriptorKind: DescriptorKind): ValidatedNel[ValidationErrorType, Unit] = {
    val s = descriptorKind match {
      case COMPONENT_DESCRIPTOR => Right(())
      case _                    => Left(InvalidDescriptorKind(descriptorKind))
    }
    s.toValidatedNel
  }

  private def validateSource(
      componentDescriptor: ComponentDescriptor
  ): ValidatedNel[ValidationErrorType, AirbyteSource] = {
    val cSource = componentDescriptor.compSpecific.hcursor.downField("source").as[Json].left
      .map(_ => MissingSpecificField("source")).toValidatedNel
    cSource.andThen(jsonSource =>
      (
        jsonSource.hcursor.downField("name").as[String].leftMap(_ => MissingSpecificSourceField("name")).toValidatedNel,
        jsonSource.hcursor.downField("connectionConfiguration").as[Json]
          .leftMap(_ => MissingSpecificSourceField("connectionConfiguration")).toValidatedNel
      ).mapN((name, connectionConfiguration) => AirbyteSource(name, connectionConfiguration, jsonSource))
    )
  }

  private def validateDestination(
      componentDescriptor: ComponentDescriptor
  ): ValidatedNel[ValidationErrorType, AirbyteDestination] = {
    val cDestination = componentDescriptor.compSpecific.hcursor.downField("destination").as[Json].left
      .map(_ => MissingSpecificField("destination")).toValidatedNel
    cDestination.andThen(jsonDestination =>
      (
        jsonDestination.hcursor.downField("name").as[String].leftMap(_ => MissingSpecificDestinationField("name"))
          .toValidatedNel,
        jsonDestination.hcursor.downField("connectionConfiguration").as[Json]
          .leftMap(_ => MissingSpecificDestinationField("connectionConfiguration")).toValidatedNel
      ).mapN((name, connectionConfiguration) => AirbyteDestination(name, connectionConfiguration, jsonDestination))
    )
  }

  private def validateConnection(
      componentDescriptor: ComponentDescriptor
  ): ValidatedNel[ValidationErrorType, AirbyteConnection] = {
    val cConnection = componentDescriptor.compSpecific.hcursor.downField("connection").as[Json]
      .leftMap(_ => MissingSpecificField("connection")).toValidatedNel
    cConnection.andThen(jsonConnection =>
      jsonConnection.hcursor.downField("name").as[String].leftMap(_ => MissingSpecificConnectionField("name"))
        .toValidatedNel.map(s => AirbyteConnection(s, jsonConnection))
    )
  }

  private def validateDataProductFields(
      componentDescriptor: ComponentDescriptor
  ): ValidatedNel[ValidationErrorType, DataProductFields] = (
    componentDescriptor.dpHeader.hcursor.downField(NAME).as[String].leftMap(_ => MissingHeaderField(NAME))
      .toValidatedNel,
    componentDescriptor.dpHeader.hcursor.downField(DOMAIN).as[String].leftMap(_ => MissingHeaderField(DOMAIN))
      .toValidatedNel,
    componentDescriptor.dpHeader.hcursor.downField(VERSION).as[String].leftMap(_ => MissingHeaderField(VERSION))
      .toValidatedNel
  ).mapN((name, domain, version) => DataProductFields(name, domain, version))

}
