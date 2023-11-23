package it.agilelab.datamesh.airbytespecificprovisioner.error

sealed trait ProvisionErrorType extends ErrorType

case class GetIdFromCreationErrorType(error: String) extends ProvisionErrorType {
  override def errorMessage: String = error
}

case class GetConnectionInfoErrorType(error: String) extends ProvisionErrorType {
  override def errorMessage: String = error
}
