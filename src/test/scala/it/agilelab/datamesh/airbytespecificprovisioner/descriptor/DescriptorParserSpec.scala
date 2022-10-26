package it.agilelab.datamesh.airbytespecificprovisioner.descriptor

import io.circe.Json
import it.agilelab.datamesh.airbytespecificprovisioner.model.ValidationError
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.EitherValues._
import org.scalatest.matchers.should.Matchers._
import it.agilelab.datamesh.airbytespecificprovisioner.common.test.getTestResourceAsString

class DescriptorParserSpec extends AnyFlatSpec {

  "Parsing a well formed descriptor" should "return a correct ComponentDescriptor" in {
    val descriptor = getTestResourceAsString("pr_descriptors/pr_descriptor_1.yml")

    val dpHeaderAndComponent = ComponentExtractor.extract(descriptor).toOption.get

    val component = ComponentDescriptor(dpHeaderAndComponent._1, dpHeaderAndComponent._2)

    component.isRight shouldBe true
  }

  "Parsing a wrongly formed descriptor with missing component id field" should "return a Left with a Exception" in {

    val descriptor = getTestResourceAsString("pr_descriptors/pr_descriptor_1_missing_component_id.yml")

    val dp: Either[ValidationError, (Json, Json)] = ComponentExtractor.extract(descriptor)

    dp.left.value.errors.head shouldBe "Input data product descriptor is invalid"

  }

  "Parsing a wrongly formed descriptor with missing components section" should "return a Left with a Exception" in {

    val descriptor = getTestResourceAsString("pr_descriptors/pr_descriptor_1_missing_components.yml")

    val dp: Either[ValidationError, (Json, Json)] = ComponentExtractor.extract(descriptor)

    dp.left.value.errors.head shouldBe "Input data product descriptor is invalid"

  }

  "Parsing a wrongly formed descriptor with missing specific section in component" should
    "return a Left with a Exception" in {

      val descriptor = getTestResourceAsString("pr_descriptors/pr_descriptor_1_missing_specific.yml")

      val dpHeaderAndComponent = ComponentExtractor.extract(descriptor).toOption.get

      val component = ComponentDescriptor(dpHeaderAndComponent._1, dpHeaderAndComponent._2)

      component.left.value.errors.head shouldBe
        "The workload urn:dmb:cmp:finance:cashflow:0:cashflows-calculation descriptor is not valid"

    }

  "Parsing a totally wrongly formed descriptor" should "return a Left with a ParsingFailure" in {

    val descriptor = """name: Marketing-Invoice-1
                       |[]
                       |""".stripMargin

    val dp: Either[ValidationError, (Json, Json)] = ComponentExtractor.extract(descriptor)

    dp.left.value.errors.head shouldBe "Input data product descriptor cannot be parsed"
  }

}
