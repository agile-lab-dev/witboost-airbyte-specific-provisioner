package it.agilelab.datamesh.airbytespecificprovisioner.api.interpreter

import it.agilelab.datamesh.airbytespecificprovisioner.api.intepreter.ModelConverter
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
}
