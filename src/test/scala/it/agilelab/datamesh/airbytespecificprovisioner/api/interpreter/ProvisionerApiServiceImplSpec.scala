package it.agilelab.datamesh.airbytespecificprovisioner.api.interpreter

import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.Behaviors
import akka.http.scaladsl.model.{HttpEntity, HttpResponse, StatusCodes}
import akka.http.scaladsl.server.{Directive1, RequestContext, Route}
import akka.http.scaladsl.testkit.{RouteTestTimeout, ScalatestRouteTest}
import akka.http.scaladsl.unmarshalling.Unmarshaller
import akka.testkit.TestDuration
import cats.data.Validated
import cats.data.Validated.Valid
import de.heikoseeberger.akkahttpcirce.FailFastCirceSupport
import io.circe.Json
import it.agilelab.datamesh.airbytespecificprovisioner.api.SpecificProvisionerApi
import it.agilelab.datamesh.airbytespecificprovisioner.api.intepreter.{
  ProvisionerApiMarshallerImpl,
  ProvisionerApiServiceImpl
}
import it.agilelab.datamesh.airbytespecificprovisioner.common.test.getTestResourceAsString
import it.agilelab.datamesh.airbytespecificprovisioner.error.{InvalidDescriptor, InvalidResponse}
import it.agilelab.datamesh.airbytespecificprovisioner.integrator.WorkloadManager
import it.agilelab.datamesh.airbytespecificprovisioner.model._
import it.agilelab.datamesh.airbytespecificprovisioner.server.Controller
import it.agilelab.datamesh.airbytespecificprovisioner.status.GetStatus
import org.scalamock.scalatest.MockFactory
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfterAll, OneInstancePerTest, OptionValues}

import scala.concurrent.duration.DurationInt

class ProvisionerApiServiceImplSpec
    extends AnyFlatSpec
    with ScalatestRouteTest
    with Matchers
    with BeforeAndAfterAll
    with FailFastCirceSupport
    with OptionValues
    with MockFactory
    with OneInstancePerTest {

  implicit val actorSystem: ActorSystem[_] = ActorSystem[Nothing](Behaviors.empty, "ProvisionerApiServiceImplSpec")

  implicit def default(implicit system: ActorSystem[Nothing]): RouteTestTimeout =
    RouteTestTimeout(new DurationInt(5).second.dilated(system.classicSystem))

  val marshaller = new ProvisionerApiMarshallerImpl

  private val airbyteManager = mock[WorkloadManager]

  private val getStatus = mock[GetStatus]

  val api = new SpecificProvisionerApi(
    new ProvisionerApiServiceImpl(airbyteManager, getStatus),
    new ProvisionerApiMarshallerImpl,
    new ExtractContexts {

      override def tapply(f: (Tuple1[Seq[(String, String)]]) => Route): Route = { (rc: RequestContext) =>
        f(Tuple1(Seq.empty))(rc)
      }
    }
  )

  new Controller(api, validationExceptionToRoute = None)(system)

  import marshaller._

  implicit val contexts: Seq[(String, String)] = Seq.empty

  "ProvisionerApiServiceImpl" should
    "synchronously validate with no errors when a valid descriptor is passed as input" in {
      val request = ProvisioningRequest(COMPONENT_DESCRIPTOR, descriptor = "valid", removeData = false)

      val _ = (airbyteManager.validate _).expects(*, *).returns(Valid(AirbyteFields(
        DataProductFields("", "", ""),
        AirbyteSource("", Json.Null, Json.Null),
        AirbyteDestination("", Json.Null, Json.Null),
        AirbyteConnection("", Json.Null)
      )))

      Post("/v1/validate", request) ~> api.route ~> check {
        val response = responseAs[ValidationResult]
        response.valid shouldEqual true
        response.error shouldBe None
      }
    }

  it should "return a validation error if validation fails" in {
    val request = ProvisioningRequest(COMPONENT_DESCRIPTOR, descriptor = "invalid", removeData = false)

    val _ = (airbyteManager.validate _).expects(*, *).returns(Validated.invalidNel(InvalidDescriptor()))

    Post("/v1/validate", request) ~> api.route ~> check {
      val response = responseAs[ValidationResult]
      response.valid shouldEqual false
      response.error.isDefined shouldBe true
    }
  }

  it should "raise an error if there's an uncaught exception while validating" in {
    val yaml    = getTestResourceAsString("pr_descriptors/pr_descriptor_1.yml")
    val request = ProvisioningRequest(COMPONENT_DESCRIPTOR, descriptor = yaml, removeData = false)

    val _ = (airbyteManager.validate _).expects(*, *).throws(new Throwable(""))

    Post("/v1/validate", request) ~> api.route ~> check(response.status shouldEqual StatusCodes.InternalServerError)
  }

  it should "synchronously provision when a valid descriptor is passed as input" in {
    val yaml    = getTestResourceAsString("pr_descriptors/pr_descriptor_1.yml")
    val request = ProvisioningRequest(COMPONENT_DESCRIPTOR, descriptor = yaml, removeData = false)

    val _ = (airbyteManager.provision _).expects(*, *).returns(Valid(ProvisionResult.completed()))

    Post("/v1/provision", request) ~> api.route ~> check {
      val response = responseAs[ProvisioningStatus]
      response.status shouldEqual ProvisioningStatusEnums.StatusEnum.COMPLETED
    }
  }

  it should "asynchronously provision when a valid descriptor is passed as input" in {
    val yaml    = getTestResourceAsString("pr_descriptors/pr_descriptor_1.yml")
    val request = ProvisioningRequest(COMPONENT_DESCRIPTOR, descriptor = yaml, removeData = false)
    val token   = ComponentToken("token-123")
    val _       = (airbyteManager.provision _).expects(*, *).returns(Valid(ProvisionResult.running(token)))

    Post("/v1/provision", request) ~> api.route ~> check {
      implicit val responseBodyUnmarshaller: Unmarshaller[HttpResponse, String] = Unmarshaller
        .strict[HttpResponse, HttpEntity](_.entity).andThen(Unmarshaller.stringUnmarshaller)
      response.status shouldEqual StatusCodes.Accepted
      val receivedToken                                                         = responseAs[String]
      receivedToken shouldEqual token.asString
    }
  }

  it should "raise an error if provision received descriptor is not valid" in {
    val request = ProvisioningRequest(COMPONENT_DESCRIPTOR, descriptor = "invalid", removeData = false)

    val _ = (airbyteManager.provision _).expects(*, *).returns(Validated.invalidNel(InvalidDescriptor()))

    Post("/v1/provision", request) ~> api.route ~> check(response.status shouldEqual StatusCodes.BadRequest)
  }

  it should "raise an error if underlying provisioning fails" in {
    val yaml    = getTestResourceAsString("pr_descriptors/pr_descriptor_1.yml")
    val request = ProvisioningRequest(COMPONENT_DESCRIPTOR, descriptor = yaml, removeData = false)

    val _ = (airbyteManager.provision _).expects(*, *).returns(Validated.invalidNel(InvalidResponse("")))

    Post("/v1/provision", request) ~> api.route ~> check(response.status shouldEqual StatusCodes.BadRequest)
  }

  it should "raise an error if there's an uncaught exception while provisioning" in {
    val yaml    = getTestResourceAsString("pr_descriptors/pr_descriptor_1.yml")
    val request = ProvisioningRequest(COMPONENT_DESCRIPTOR, descriptor = yaml, removeData = false)

    val _ = (airbyteManager.provision _).expects(*, *).throws(new Throwable(""))

    Post("/v1/provision", request) ~> api.route ~> check(response.status shouldEqual StatusCodes.InternalServerError)
  }

  it should "synchronously unprovision when a valid descriptor is passed as input" in {
    val yaml    = getTestResourceAsString("pr_descriptors/pr_descriptor_1.yml")
    val request = ProvisioningRequest(COMPONENT_DESCRIPTOR, descriptor = yaml, removeData = false)

    val _ = (airbyteManager.unprovision _).expects(*, *).returns(Valid(ProvisionResult.completed()))

    Post("/v1/unprovision", request) ~> api.route ~> check {
      val response = responseAs[ProvisioningStatus]
      response.status shouldEqual ProvisioningStatusEnums.StatusEnum.COMPLETED
    }
  }

  it should "asynchronously unprovision when a valid descriptor is passed as input" in {
    val yaml    = getTestResourceAsString("pr_descriptors/pr_descriptor_1.yml")
    val request = ProvisioningRequest(COMPONENT_DESCRIPTOR, descriptor = yaml, removeData = false)
    val token   = ComponentToken("token-123")
    val _       = (airbyteManager.unprovision _).expects(*, *).returns(Valid(ProvisionResult.running(token)))

    Post("/v1/unprovision", request) ~> api.route ~> check {
      implicit val responseBodyUnmarshaller: Unmarshaller[HttpResponse, String] = Unmarshaller
        .strict[HttpResponse, HttpEntity](_.entity).andThen(Unmarshaller.stringUnmarshaller)
      response.status shouldEqual StatusCodes.Accepted
      val receivedToken                                                         = responseAs[String]
      receivedToken shouldEqual token.asString
    }
  }

  it should "raise an error if unprovision received descriptor is not valid" in {
    val request = ProvisioningRequest(COMPONENT_DESCRIPTOR, descriptor = "invalid", removeData = false)

    val _ = (airbyteManager.unprovision _).expects(*, *).returns(Validated.invalidNel(InvalidDescriptor()))

    Post("/v1/unprovision", request) ~> api.route ~> check(response.status shouldEqual StatusCodes.BadRequest)
  }

  it should "raise an error if underlying unprovisioning fails" in {
    val yaml    = getTestResourceAsString("pr_descriptors/pr_descriptor_1.yml")
    val request = ProvisioningRequest(COMPONENT_DESCRIPTOR, descriptor = yaml, removeData = false)

    val _ = (airbyteManager.unprovision _).expects(*, *).returns(Validated.invalidNel(InvalidResponse("")))

    Post("/v1/unprovision", request) ~> api.route ~> check(response.status shouldEqual StatusCodes.BadRequest)
  }

  it should "raise an error if there's an uncaught exception while unprovisioning" in {
    val yaml    = getTestResourceAsString("pr_descriptors/pr_descriptor_1.yml")
    val request = ProvisioningRequest(COMPONENT_DESCRIPTOR, descriptor = yaml, removeData = false)

    val _ = (airbyteManager.unprovision _).expects(*, *).throws(new Throwable(""))

    Post("/v1/unprovision", request) ~> api.route ~> check(response.status shouldEqual StatusCodes.InternalServerError)
  }

  it should "raise an error for an updateAcl request" in {
    val request = UpdateAclRequest(List("sergio.mejia_agilelab.it"), ProvisionInfo("req", "res"))

    Post("/v1/updateacl", request) ~> api.route ~> check(response.status shouldEqual StatusCodes.InternalServerError)
  }

  it should "return a provision result for an existing async getStatus request" in {
    val _ = (getStatus.statusOf _).expects(ComponentToken("token-123")).returns(Some(ProvisionResult.completed()))
    Get("/v1/provision/token-123/status") ~> api.route ~> check {
      val response = responseAs[ProvisioningStatus]
      response.status shouldEqual ProvisioningStatusEnums.StatusEnum.COMPLETED
    }
  }

  it should "return an error if an operation with received token doesn't exist" in {
    val _ = (getStatus.statusOf _).expects(ComponentToken("token-123")).returns(None)
    Get("/v1/provision/token-123/status") ~> api.route ~> check {
      response.status shouldEqual StatusCodes.BadRequest
      val content = responseAs[RequestValidationError]
      content.errors should contain("Couldn't find operation for token 'token-123'")
    }
  }

  it should "raise an error for an async validate request" in {
    val request = ProvisioningRequest(COMPONENT_DESCRIPTOR, descriptor = "", removeData = false)

    Post("/v2/validate", request) ~> api.route ~> check(response.status shouldEqual StatusCodes.InternalServerError)
  }

  it should "raise an error for a validate status request" in Get("/v2/validate/token/status") ~> api.route ~> check {
    response.status shouldEqual StatusCodes.InternalServerError
  }

  it should "raise an error for a reverse provisioning request" in {
    val request = ReverseProvisioningRequest("", "")

    Post("/v1/reverse-provisioning", request) ~> api.route ~> check {
      response.status shouldEqual StatusCodes.InternalServerError
    }
  }

  it should "raise an error for a reverse provisioning status request" in
    Get("/v1/reverse-provisioning/token/status") ~> api.route ~> check {
      response.status shouldEqual StatusCodes.InternalServerError
    }

}

abstract class ExtractContexts extends Directive1[Seq[(String, String)]]

object ExtractContexts {

  def apply(other: Directive1[Seq[(String, String)]]): ExtractContexts = inner =>
    other.tapply((seq: Tuple1[Seq[(String, String)]]) =>
      (rc: RequestContext) => {
        val headers = rc.request.headers.map(header => (header.name(), header.value()))
        inner(Tuple1(seq._1 ++ headers))(rc)
      }
    )
}
