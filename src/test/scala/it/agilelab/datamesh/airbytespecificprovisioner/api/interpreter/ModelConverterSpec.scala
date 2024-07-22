package it.agilelab.datamesh.airbytespecificprovisioner.api.interpreter

import cats.data.NonEmptyList
import it.agilelab.datamesh.airbytespecificprovisioner.api.intepreter.ModelConverter
import it.agilelab.datamesh.airbytespecificprovisioner.error.{ErrorType, InvalidResponse}
import it.agilelab.datamesh.airbytespecificprovisioner.model.ValidationError
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

class ModelConverterSpec extends AnyFlatSpec with should.Matchers {

  it should "convert correctly a validation error" in {
    val err = ValidationError(List("Error1", "Error2"))

    val valErr = ModelConverter.buildRequestValidationError(err)
    valErr.errors should contain theSameElementsAs err.errors
    valErr.moreInfo shouldNot be(None)
    valErr.moreInfo.get.problems shouldEqual err.errors
  }

  it should "correctly create a validation error from a list of ErrorType" in {
    val err: NonEmptyList[ErrorType] = NonEmptyList.of(InvalidResponse("Error"))

    val valErr = ModelConverter.buildRequestValidationError(err)

    valErr.userMessage shouldEqual
      Some("Validation failed on the received descriptor. Check the error details for more information")
    valErr.errors should contain theSameElementsAs err.map(_.errorMessage).toList
    valErr.moreInfo shouldNot be(None)
    valErr.moreInfo.get.problems shouldEqual err.map(_.errorMessage).toList
    valErr.moreInfo.get.solutions shouldNot be(empty)
  }

  it should "correctly create a system error from a throwable" in {
    val err = new Exception("Error!")

    val valErr = ModelConverter.buildSystemError(err)

    valErr.userMessage shouldEqual
      Some("An unexpected error occurred while processing the request. Check the error details for more information.")
    valErr.error.contains(err.getMessage) shouldBe true
    valErr.moreInfo shouldNot be(None)
    valErr.moreInfo.get.problems should contain(err.getMessage)
    valErr.moreInfo.get.solutions shouldNot be(empty)
  }
}
