package it.agilelab.datamesh.airbytespecificprovisioner.api.intepreter

import it.agilelab.datamesh.airbytespecificprovisioner.model.{ErrorMoreInfo, RequestValidationError, ValidationError}

object ModelConverter {

  // TODO To be refactored when we implement good error handling
  def buildRequestValidationError(airbyteError: ValidationError): RequestValidationError = RequestValidationError(
    errors = airbyteError.errors,
    userMessage = Some("Error on Airbyte Specific Provisioner"),
    input = None,
    inputErrorField = None,
    moreInfo = Some(ErrorMoreInfo(problems = airbyteError.errors, solutions = List.empty))
  )
}
