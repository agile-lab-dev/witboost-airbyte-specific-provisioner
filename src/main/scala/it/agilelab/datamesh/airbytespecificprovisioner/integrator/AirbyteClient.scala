package it.agilelab.datamesh.airbytespecificprovisioner.integrator

import akka.actor
import akka.actor.typed.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.{Authorization, BasicHttpCredentials}
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.stream.Materializer
import com.typesafe.scalalogging.StrictLogging
import io.circe.{parser, Json, JsonObject}
import it.agilelab.datamesh.airbytespecificprovisioner.common.Constants.{
  CREATE_ACTION,
  DELETE_ACTION,
  DISCOVER_ACTION,
  LIST_ACTION
}
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
      maybeStringRequest: Option[String],
      username: String,
      password: String,
      authEnabled: Boolean
  ): Future[HttpResponse] = {
    logger.info(
      "Calling method {} on api {} with body: [{}]",
      httpMethod.value,
      uri.toString,
      maybeStringRequest.getOrElse("")
    )
    val httpRequest =
      if (authEnabled) {
        HttpRequest(method = httpMethod, uri = uri).withHeaders(Authorization(BasicHttpCredentials(username, password)))
      } else { HttpRequest(method = httpMethod, uri = uri) }
    maybeStringRequest match {
      case Some(stringRequest) => Http(system)
          .singleRequest(httpRequest.withEntity(HttpEntity(ContentTypes.`application/json`, stringRequest)))
      case None                => Http(system).singleRequest(httpRequest)
    }
  }

  def httpResponseUnmarshaller(httpResponse: HttpResponse): Try[String] = Try {
    val unmarshalledFutureResponse = Unmarshal(httpResponse.entity).to[String]
    Await.result(unmarshalledFutureResponse, ApplicationConfiguration.airbyteConfiguration.invocationTimeout seconds)
  }

  def createOrRecreate(workspaceId: String, jsonRequest: Json, resourceType: String): Either[Product, String] = {
    delete(workspaceId, jsonRequest, resourceType)
    submitRequest(jsonRequest, resourceType, CREATE_ACTION)
  }

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

  def discoverSchema(sourceId: String): Either[SystemError, String] = submitRequest(
    Json.obj(("sourceId", Json.fromString(sourceId)), ("disable_cache", Json.fromBoolean(false))),
    "source",
    DISCOVER_ACTION
  )

  def submitRequest(jsonRequest: Json, resource: String, action: String): Either[SystemError, String] = {
    val futureResponse = buildFutureHttpResponse(
      HttpMethods.POST,
      Uri(Seq(ApplicationConfiguration.airbyteConfiguration.baseUrl, s"${resource}s", action).mkString("/")),
      Some(jsonRequest.toString),
      ApplicationConfiguration.airbyteConfiguration.airbyteUser,
      ApplicationConfiguration.airbyteConfiguration.airbytePassword,
      ApplicationConfiguration.airbyteConfiguration.basicAuth.replace("'", "").toBooleanOption.getOrElse(false)
    )

    val tryHttpResponse =
      Try(Await.result(futureResponse, ApplicationConfiguration.airbyteConfiguration.invocationTimeout seconds))

    tryHttpResponse match {
      case Failure(e)            =>
        logger.error(s"No response received from Airbyte while invoking ${resource}s/$action endpoint", e)
        Left(SystemError(
          s"No response received from Airbyte while invoking ${resource}s/$action endpoint. Please try again, if the issue persists contact the platform team"
        ))
      case Success(httpResponse) => httpResponseUnmarshaller(httpResponse) match {
          case Success(response) =>
            logger.info(
              s"Response from Airbyte after invoking ${resource}s/$action endpoint: (${httpResponse.status.value}) $response"
            )
            httpResponse.status.intValue() match {
              case 200 | 204 => Right(response)
              case 400       =>
                Left(SystemError(s"Airbyte failed to validate request for ${resource}s/$action endpoint: $response"))
              case _         => Left(SystemError(
                  s"Request to ${resource}s/$action endpoint failed with error: ${httpResponse.status.toString}"
                ))
            }
          case Failure(e)        =>
            logger.error("Failed to unmarshal the Airbyte response", e)
            Left(SystemError("Failed to unmarshal the Airbyte response, contact the platform team for assistance"))
        }
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
