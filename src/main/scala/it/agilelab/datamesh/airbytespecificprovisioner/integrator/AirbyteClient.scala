package it.agilelab.datamesh.airbytespecificprovisioner.integrator

import akka.actor
import akka.actor.typed.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.stream.Materializer
import com.typesafe.scalalogging.StrictLogging
import io.circe.{parser, Json, JsonObject}
import it.agilelab.datamesh.airbytespecificprovisioner.common.Constants.{CREATE_ACTION, DELETE_ACTION, LIST_ACTION}
import it.agilelab.datamesh.airbytespecificprovisioner.model.{SystemError, ValidationError}
import it.agilelab.datamesh.airbytespecificprovisioner.system.ApplicationConfiguration

import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.language.postfixOps
import scala.util.{Failure, Success, Try}

class AirbyteClient(system: ActorSystem[_]) extends StrictLogging {

  implicit val classic: actor.ActorSystem         = system.classicSystem
  implicit val executionContext: ExecutionContext = system.executionContext
  implicit val materializer: Materializer         = Materializer.matFromSystem(classic)

  private def buildFutureHttpResponse(
      httpMethod: HttpMethod,
      uri: Uri,
      maybeStringRequest: Option[String]
  ): Future[HttpResponse] = {
    logger.info(
      "Calling method {} on api {} with body: [{}]",
      httpMethod.value,
      uri.toString,
      maybeStringRequest.getOrElse("")
    )
    val httpRequest = HttpRequest(method = httpMethod, uri = uri)
    maybeStringRequest match {
      case Some(stringRequest) => Http(system)
          .singleRequest(httpRequest.withEntity(HttpEntity(ContentTypes.`application/json`, stringRequest)))
      case None                => Http(system).singleRequest(httpRequest)
    }
  }

  def httpResponseUnmarshaller(httpResponse: HttpResponse): Try[String] = Try {
    val unmarshalledFutureResponse = Unmarshal(httpResponse.entity).to[String]
    Await.result(unmarshalledFutureResponse, ApplicationConfiguration.airbyteInvocationTimeout seconds)
  }

  def createOrRecreate(workspaceId: String, jsonRequest: Json, resourceType: String): Either[Product, String] = for {
    _                      <- delete(workspaceId, jsonRequest, resourceType)
    createResourceResponse <- submitRequest(jsonRequest, resourceType, CREATE_ACTION)
  } yield createResourceResponse

  def delete(workspaceId: String, jsonRequest: Json, resourceType: String): Either[Product, String] = for {
    name <- jsonRequest.hcursor.downField("name").as[String].left.map(_ => ValidationError(Seq("name not found")))
    listResourcesResponse          <-
      submitRequest(Json.obj(("workspaceId", Json.fromString(workspaceId))), resourceType, LIST_ACTION)
    maybeAlreadyExistingResourceId <- getMaybeAlreadyExistingResourceId(listResourcesResponse, resourceType, name)
    deleteResourceResponse         <- maybeAlreadyExistingResourceId match {
      case Some(alreadyExistingResourceId) => submitRequest(
          Json.obj((s"${resourceType}Id", Json.fromString(alreadyExistingResourceId))),
          resourceType,
          DELETE_ACTION
        )
      case None                            => Right("")
    }
  } yield deleteResourceResponse

  def submitRequest(jsonRequest: Json, resource: String, action: String): Either[SystemError, String] = {
    val futureResponse = buildFutureHttpResponse(
      HttpMethods.POST,
      Uri(Seq(ApplicationConfiguration.airbyteBaseUrl, s"${resource}s", action).mkString("/")),
      Some(jsonRequest.toString)
    )
    val httpResponse   = Await.result(futureResponse, ApplicationConfiguration.airbyteInvocationTimeout seconds)
    httpResponseUnmarshaller(httpResponse) match {
      case Success(response) => httpResponse.status.intValue() match {
          case 200 | 204 => Right(response)
          case 400       =>
            Left(SystemError(s"Airbyte failed to validate request for ${resource}s/$action endpoint: $response"))
          case _         => Left(SystemError(
              s"Request to ${resource}s/$action endpoint failed with error: ${httpResponse.status.toString}"
            ))
        }
      case Failure(e)        => Left(SystemError(e.getMessage))
    }
  }

  private def getMaybeAlreadyExistingResourceId(
      jsonResponse: String,
      resourceType: String,
      name: String
  ): Either[SystemError, Option[String]] = parser.parse(jsonResponse) match {
    case Right(response) => response.hcursor.downField(s"${resourceType}s").as[List[JsonObject]] match {
        case Right(l) => l.filter(jo => jo("name").contains(Json.fromString(name))) match {
            case r :: Nil        => Right(r(s"${resourceType}Id").flatMap(_.asString))
            case l if l.nonEmpty => Left(SystemError("Failed to parse response"))
            case _               => Right(None)
          }
        case Left(_)  => Right(None)
      }
    case Left(_)         => Left(SystemError("Failed to parse response"))
  }

}
