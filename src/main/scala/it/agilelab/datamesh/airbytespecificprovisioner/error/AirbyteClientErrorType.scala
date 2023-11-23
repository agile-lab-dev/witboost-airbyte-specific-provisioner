package it.agilelab.datamesh.airbytespecificprovisioner.error

sealed trait AirbyteClientErrorType extends ErrorType

case class FailureResponse(message: String, error: Throwable) extends AirbyteClientErrorType {
  override def errorMessage: String = message
}

case class InvalidResponse(message: String) extends AirbyteClientErrorType {
  override def errorMessage: String = message
}
