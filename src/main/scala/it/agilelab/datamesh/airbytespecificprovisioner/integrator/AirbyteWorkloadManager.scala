package it.agilelab.datamesh.airbytespecificprovisioner.integrator

import com.typesafe.scalalogging.StrictLogging
import io.circe.yaml.{parser => yamlParser}
import it.agilelab.datamesh.airbytespecificprovisioner.descriptor.ComponentDescriptor
import it.agilelab.datamesh.airbytespecificprovisioner.model.{COMPONENT_DESCRIPTOR, DescriptorKind, ValidationError}

import scala.annotation.nowarn

class AirbyteWorkloadManager extends StrictLogging {

  private def provisionComponent(componentDescriptor: ComponentDescriptor): Either[Product, String] = ???

  private def runTask(
      componentDescriptor: String,
      @nowarn
      descriptorKind: DescriptorKind,
      @nowarn
      opApplication: ComponentDescriptor => Either[Product, String]
  ): Either[Product, String] = {
    val result = for {
      parsedYaml          <- yamlParser.parse(componentDescriptor)
      componentDescriptor <- ComponentDescriptor(parsedYaml)
      specificConfig      <- componentDescriptor.getComponentSpecific
    } yield specificConfig.toString
    logger.info(result.toString)
    result
  }

  def validate(descriptorKind: DescriptorKind, descriptor: String): Either[Product, Boolean] = ???

  def provision(descriptorKind: DescriptorKind, descriptor: String): Either[Product, String] = descriptorKind match {
    case COMPONENT_DESCRIPTOR =>
      logger.info("Invoking method {}", "provisionComponent")
      runTask(descriptor, descriptorKind, provisionComponent)
        .flatMap(responses => Right(s"[${responses.mkString(", ")}]"))
    case _ => Left(ValidationError(Seq("Descriptor kind must be COMPONENT_DESCRIPTOR for /provision API")))
  }

}
