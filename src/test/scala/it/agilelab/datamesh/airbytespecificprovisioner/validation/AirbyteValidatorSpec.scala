package it.agilelab.datamesh.airbytespecificprovisioner.validation

import cats.data.Validated
import it.agilelab.datamesh.airbytespecificprovisioner.common.Constants.{DOMAIN, NAME, VERSION}
import it.agilelab.datamesh.airbytespecificprovisioner.common.test.getTestResourceAsString
import it.agilelab.datamesh.airbytespecificprovisioner.error.{
  InvalidDescriptorKind,
  MissingHeaderField,
  MissingSpecificConnectionField,
  MissingSpecificDestinationField,
  MissingSpecificField,
  MissingSpecificSourceField
}
import it.agilelab.datamesh.airbytespecificprovisioner.model.{COMPONENT_DESCRIPTOR, DATAPRODUCT_DESCRIPTOR}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class AirbyteValidatorSpec extends AnyFlatSpec with Matchers {

  "Validator" should "return a success if descriptor is valid" in {
    val descriptor = getTestResourceAsString("pr_descriptors/pr_descriptor_1.yml")
    val validator  = new AirbyteValidator()

    val res = validator.validate(COMPONENT_DESCRIPTOR, descriptor)

    res match {
      case Validated.Valid(ai)  =>
        ai.source.name shouldBe "Public Places Assaults CSV"
        ai.destination.name shouldBe "Snowflake"
        ai.connection.name shouldBe "Public Places Assaults CSV <> Snowflake"
        ai.dpFields.name shouldBe "CashFlow"
        ai.dpFields.version shouldBe "0.2.0"
        ai.dpFields.domain shouldBe "finance"
      case Validated.Invalid(_) => fail("Expected valid")
    }
  }

  it should "return a failure if descriptor kind is invalid" in {
    val descriptor = getTestResourceAsString("pr_descriptors/pr_descriptor_1.yml")
    val validator  = new AirbyteValidator()

    val res = validator.validate(DATAPRODUCT_DESCRIPTOR, descriptor)

    res match {
      case Validated.Valid(_)   => fail("Expected invalid")
      case Validated.Invalid(e) => e.head shouldBe InvalidDescriptorKind(DATAPRODUCT_DESCRIPTOR)
    }
  }

  it should "return a failure if specific is empty" in {
    val descriptor = getTestResourceAsString("pr_descriptors/pr_descriptor_1_empty_specific.yml")
    val validator  = new AirbyteValidator()

    val res = validator.validate(COMPONENT_DESCRIPTOR, descriptor)

    res match {
      case Validated.Valid(_)   => fail("Expected invalid")
      case Validated.Invalid(e) => (e.toList should contain).allOf(
          MissingSpecificField("source"),
          MissingSpecificField("destination"),
          MissingSpecificField("connection")
        )
    }
  }

  it should "return a failure if specific.source is empty" in {
    val descriptor = getTestResourceAsString("pr_descriptors/pr_descriptor_1_missing_source.yml")
    val validator  = new AirbyteValidator()

    val res = validator.validate(COMPONENT_DESCRIPTOR, descriptor)

    res match {
      case Validated.Valid(_)   => fail("Expected invalid")
      case Validated.Invalid(e) => (e.toList should contain)
          .allOf(MissingSpecificSourceField("name"), MissingSpecificSourceField("connectionConfiguration"))
    }
  }

  it should "return a failure if specific.destination is empty" in {
    val descriptor = getTestResourceAsString("pr_descriptors/pr_descriptor_1_missing_destination.yml")
    val validator  = new AirbyteValidator()

    val res = validator.validate(COMPONENT_DESCRIPTOR, descriptor)

    res match {
      case Validated.Valid(_)   => fail("Expected invalid")
      case Validated.Invalid(e) => (e.toList should contain)
          .allOf(MissingSpecificDestinationField("name"), MissingSpecificDestinationField("connectionConfiguration"))
    }
  }

  it should "return a failure if specific.connection.name is not present" in {
    val descriptor = getTestResourceAsString("pr_descriptors/pr_descriptor_1_missing_connection_name.yml")
    val validator  = new AirbyteValidator()

    val res = validator.validate(COMPONENT_DESCRIPTOR, descriptor)

    res match {
      case Validated.Valid(_)   => fail("Expected invalid")
      case Validated.Invalid(e) => e.toList should contain(MissingSpecificConnectionField("name"))
    }
  }

  it should "return a failure if expected DP fields are not present" in {
    val descriptor = getTestResourceAsString("pr_descriptors/pr_descriptor_1_missing_expected_fields.yml")
    val validator  = new AirbyteValidator()

    val res = validator.validate(COMPONENT_DESCRIPTOR, descriptor)

    res match {
      case Validated.Valid(_)   => fail("Expected invalid")
      case Validated.Invalid(e) => (e.toList should contain)
          .allOf(MissingHeaderField(NAME), MissingHeaderField(DOMAIN), MissingHeaderField(VERSION))
    }
  }

}
