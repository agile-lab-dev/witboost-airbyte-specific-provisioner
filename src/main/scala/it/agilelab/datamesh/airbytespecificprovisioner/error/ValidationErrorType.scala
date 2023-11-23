package it.agilelab.datamesh.airbytespecificprovisioner.error

import it.agilelab.datamesh.airbytespecificprovisioner.model.DescriptorKind

sealed trait ValidationErrorType extends ErrorType

case class InvalidDescriptor() extends ValidationErrorType {
  override def errorMessage: String = "Descriptor is not valid"
}

case class ParseFailureDescriptor(error: Throwable) extends ValidationErrorType {
  override def errorMessage: String = s"Failed to parse Descriptor. Details: ${error.getMessage}"
}

case class InvalidComponent(componentId: String) extends ValidationErrorType {
  override def errorMessage: String = s"The descriptor for the workload with ID $componentId is not valid"
}

case class InvalidDescriptorKind(descriptorKind: DescriptorKind) extends ValidationErrorType {
  override def errorMessage: String = s"Descriptor Kind ${descriptorKind.toString} is not supported by this Provisioner"
}

case class MissingSpecificField(field: String) extends ValidationErrorType {

  override def errorMessage: String =
    s"The field $field is not present or invalid in the specific section of the component"
}

case class MissingSpecificSourceField(field: String) extends ValidationErrorType {

  override def errorMessage: String =
    s"The field $field is not present or invalid in the specific.source section of the component"
}

case class MissingSpecificDestinationField(field: String) extends ValidationErrorType {

  override def errorMessage: String =
    s"The field $field is not present or invalid in the specific.destination section of the component"
}

case class MissingSpecificConnectionField(field: String) extends ValidationErrorType {

  override def errorMessage: String =
    s"The field $field is not present or invalid in the specific.connection section of the component"
}

case class MissingHeaderField(field: String) extends ValidationErrorType {
  override def errorMessage: String = s"The field $field is not present in the descriptor"
}
