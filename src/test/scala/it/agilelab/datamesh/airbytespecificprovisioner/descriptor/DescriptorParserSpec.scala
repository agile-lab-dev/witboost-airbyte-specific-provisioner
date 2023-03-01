package it.agilelab.datamesh.airbytespecificprovisioner.descriptor

import io.circe.Json
import it.agilelab.datamesh.airbytespecificprovisioner.model.ValidationError
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.EitherValues._
import org.scalatest.matchers.should.Matchers._
import it.agilelab.datamesh.airbytespecificprovisioner.common.test.getTestResourceAsString
import it.agilelab.datamesh.airbytespecificprovisioner.system.ApplicationConfiguration

class DescriptorParserSpec extends AnyFlatSpec {
  private val accessToken: String = ApplicationConfiguration.airbyteConfiguration.dbtGitToken
  private val userName: String    = ApplicationConfiguration.airbyteConfiguration.dbtGitUser

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

  "Parsing a well formed descriptor with dbt transformation" should "return the correct dbtGitUrl" in {
    val descriptor = getTestResourceAsString("pr_descriptors/pr_descriptor_2_dbt.yml")

    val dpHeaderAndComponent = ComponentExtractor.extract(descriptor).toOption.get

    val component = ComponentDescriptor(dpHeaderAndComponent._1, dpHeaderAndComponent._2).toOption.get

    component.getDbtGitUrl shouldBe
      f"https://$userName%s:$accessToken%s@gitlab.com/AgileFactory/Witboost.Mesh/Mesh.Repository/Sandbox/dbttesttwo.git"
  }

  "Parsing a well formed descriptor without dbt transformation" should "return an empty dbtGitUrl" in {
    val descriptor = getTestResourceAsString("pr_descriptors/pr_descriptor_1.yml")

    val dpHeaderAndComponent = ComponentExtractor.extract(descriptor).toOption.get

    val component = ComponentDescriptor(dpHeaderAndComponent._1, dpHeaderAndComponent._2).toOption.get

    component.getDbtGitUrl shouldBe ""
  }

  "Parsing a well formed descriptor with an empty dbt transformation field" should "return an empty dbtGitUrl" in {
    val descriptor = getTestResourceAsString("pr_descriptors/pr_descriptor_2_empty_dbt.yml")

    val dpHeaderAndComponent = ComponentExtractor.extract(descriptor).toOption.get

    val component = ComponentDescriptor(dpHeaderAndComponent._1, dpHeaderAndComponent._2).toOption.get

    component.getDbtGitUrl shouldBe ""
  }

  "Parsing a well formed descriptor" should "return the correct connection name" in {
    val descriptor = getTestResourceAsString("pr_descriptors/pr_descriptor_1.yml")

    val dpHeaderAndComponent = ComponentExtractor.extract(descriptor).toOption.get

    val component = ComponentDescriptor(dpHeaderAndComponent._1, dpHeaderAndComponent._2).toOption.get

    component.getConnectionName.toOption.get shouldBe "Public Places Assaults CSV <> Snowflake"
  }

  "Parsing a wrongly formed descriptor with missing connection name" should "return a Left with an Error" in {
    val descriptor = getTestResourceAsString("pr_descriptors/pr_descriptor_1_missing_connection_name.yml")

    val dpHeaderAndComponent = ComponentExtractor.extract(descriptor).toOption.get

    val component = ComponentDescriptor(dpHeaderAndComponent._1, dpHeaderAndComponent._2).toOption.get

    component.getConnectionName.left.value.errors.head shouldBe "Failed to retrieve connection name"
  }
}
