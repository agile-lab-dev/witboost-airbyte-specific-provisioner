package it.agilelab.datamesh.airbytespecificprovisioner.integrator

import com.typesafe.scalalogging.StrictLogging
import io.circe.yaml.{parser => yamlParser}
import it.agilelab.datamesh.airbytespecificprovisioner.descriptor.ComponentDescriptor
import it.agilelab.datamesh.airbytespecificprovisioner.model.{COMPONENT_DESCRIPTOR, DescriptorKind, ValidationError}

import scala.annotation.nowarn

class DataCatalogManager extends StrictLogging {

  private def provisionOutputport(
      outputportDescriptor: ComponentDescriptor
  ): Either[Product, String] = ???



  private def runTask(
                       componentDescriptor: String,
                       @nowarn descriptorKind: DescriptorKind,
                       @nowarn opApplication: ComponentDescriptor => Either[Product, String]
  ): Either[Product, String] = {
    val result = for {
      parsedYaml <- yamlParser.parse(componentDescriptor)
      //    match {
      //      case Left(_) => Left(ValidationError(List("Input workload descriptor cannot be parsed")))
      //      case Right(descriptor) => descriptor
      //    }
      componentDescriptor <- ComponentDescriptor(parsedYaml)

      specificConfig <- componentDescriptor.getComponentSpecific

      //    match {
      //      case Left(_) => Left(ValidationError(List("Input workload descriptor cannot be parsed")))
      //      case Right(descriptor) => Some(descriptor)
      //    }
    } yield specificConfig.toString

    logger.info(result.toString)

    result
  }


  def validate(descriptorKind: DescriptorKind, dataProductDescriptor: String): Either[Product, Boolean] = ???

  def provision(descriptorKind: DescriptorKind, dataProductDescriptor: String): Either[Product, String] =
    descriptorKind match {
      case COMPONENT_DESCRIPTOR =>
        logger.info("Invoking method {}", "provisionOutputport")
        runTask(dataProductDescriptor, descriptorKind, provisionOutputport)
          .flatMap(responses => Right(s"[${responses.mkString(", ")}]"))
      case _                                   =>
        Left(ValidationError(Seq("Descriptor kind must be DATAPRODUCT_DESCRIPTOR_WITH_RESULTS for provisioning API")))
    }

}
