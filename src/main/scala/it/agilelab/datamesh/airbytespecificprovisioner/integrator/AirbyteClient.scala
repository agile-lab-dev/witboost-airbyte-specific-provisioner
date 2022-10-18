package it.agilelab.datamesh.airbytespecificprovisioner.integrator

import akka.actor
import akka.actor.typed.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.stream.Materializer
import com.typesafe.scalalogging.StrictLogging
import it.agilelab.datamesh.airbytespecificprovisioner.model.SystemError
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

  def createSource(jsonRequest: String): Either[SystemError, String] = {
    val futureResponse = buildFutureHttpResponse(
      HttpMethods.POST,
      Uri(ApplicationConfiguration.airbyteSourceCreationEndpoint),
      Some(jsonRequest)
    )
    val httpResponse   = Await.result(futureResponse, ApplicationConfiguration.airbyteInvocationTimeout seconds)
    httpResponseUnmarshaller(httpResponse) match {
      case Success(response) => httpResponse.status.intValue() match {
          case 200 => Right(response)
          case 400 => Left(SystemError(s"Airbyte failed to validate request for creating source: $response"))
          case _ => Left(SystemError(s"Request for creating source failed with error: ${httpResponse.status.toString}"))
        }
      case Failure(e)        => Left(SystemError(e.getMessage))
    }
  }

  def createDestination(jsonRequest: String): Either[SystemError, String] = {
    val futureResponse = buildFutureHttpResponse(
      HttpMethods.POST,
      Uri(ApplicationConfiguration.airbyteDestinationCreationEndpoint),
      Some(jsonRequest)
    )
    val httpResponse   = Await.result(futureResponse, ApplicationConfiguration.airbyteInvocationTimeout seconds)
    httpResponseUnmarshaller(httpResponse) match {
      case Success(response) => httpResponse.status.intValue() match {
          case 200 => Right(response)
          case 400 => Left(SystemError(s"Airbyte failed to validate request for creating destination: $response"))
          case _   =>
            Left(SystemError(s"Request for creating destination failed with error: ${httpResponse.status.toString}"))
        }
      case Failure(e)        => Left(SystemError(e.getMessage))
    }
  }

  def createConnection(jsonRequest: String): Either[SystemError, String] = {
    val futureResponse = buildFutureHttpResponse(
      HttpMethods.POST,
      Uri(ApplicationConfiguration.airbyteConnectionCreationEndpoint),
      Some(jsonRequest)
    )
    val httpResponse   = Await.result(futureResponse, ApplicationConfiguration.airbyteInvocationTimeout seconds)
    httpResponseUnmarshaller(httpResponse) match {
      case Success(response) => httpResponse.status.intValue() match {
          case 200 => Right(response)
          case 400 => Left(SystemError(s"Airbyte failed to validate request for creating connection: $response"))
          case _   =>
            Left(SystemError(s"Request for creating connection failed with error: ${httpResponse.status.toString}"))
        }
      case Failure(e)        => Left(SystemError(e.getMessage))
    }
  }

}
