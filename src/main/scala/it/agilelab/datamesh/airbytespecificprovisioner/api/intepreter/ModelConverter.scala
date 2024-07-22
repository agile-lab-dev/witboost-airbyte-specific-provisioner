package it.agilelab.datamesh.airbytespecificprovisioner.api.intepreter

import cats.data.NonEmptyList
import it.agilelab.datamesh.airbytespecificprovisioner.error.ErrorType
import it.agilelab.datamesh.airbytespecificprovisioner.model.{
  ErrorMoreInfo,
  RequestValidationError,
  SystemError,
  ValidationError
}

object ModelConverter {

  // TODO To be refactored when we implement good error handling
  def buildRequestValidationError(airbyteError: ValidationError): RequestValidationError = RequestValidationError(
    errors = airbyteError.errors,
    userMessage = Some("Error on Airbyte Specific Provisioner"),
    input = None,
    inputErrorField = None,
    moreInfo = Some(ErrorMoreInfo(problems = airbyteError.errors, solutions = List.empty))
  )

  def buildRequestValidationError(errorNel: NonEmptyList[ErrorType]): RequestValidationError = {
    val errorStrings = errorNel.map(_.errorMessage).toList
    RequestValidationError(
      errors = errorStrings,
      userMessage = Some("Validation failed on the received descriptor. Check the error details for more information"),
      input = None,
      inputErrorField = None,
      moreInfo = Some(ErrorMoreInfo(
        problems = errorStrings,
        solutions =
          List("Please check that the input descriptor is compliant with the expected schema for an Airbyte component")
      ))
    )
  }

  def buildSystemError(t: Throwable): SystemError = {
    val message  =
      "An unexpected error occurred while processing the request. Check the error details for more information."
    val solution = "Please try again and if the problem persists contact the platform team."
    SystemError(
      userMessage = Some(message),
      error = s"$message $solution Details: ${t.getMessage}",
      moreInfo = Some(ErrorMoreInfo(problems = List(t.getMessage), solutions = List(solution)))
    )
  }
}
