package it.agilelab.datamesh.airbytespecificprovisioner.server.impl

import akka.actor
import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.Behaviors
import akka.http.scaladsl.Http
import akka.http.scaladsl.server.Directives.complete
import akka.http.scaladsl.server.directives.{Credentials, SecurityDirectives}
import buildinfo.BuildInfo
import com.typesafe.scalalogging.LazyLogging
import it.agilelab.datamesh.airbytespecificprovisioner.api.intepreter.{
  ProvisionerApiMarshallerImpl,
  ProvisionerApiServiceImpl
}
import it.agilelab.datamesh.airbytespecificprovisioner.api.{SpecificProvisionerApi, SpecificProvisionerApiService}
import it.agilelab.datamesh.airbytespecificprovisioner.integrator.{
  AirbyteClient,
  AirbyteWorkloadManager,
  AsyncAirbyteWorkloadManager
}
import it.agilelab.datamesh.airbytespecificprovisioner.server.Controller
import it.agilelab.datamesh.airbytespecificprovisioner.status.{GetStatus, StatusService, TaskRepository}
import it.agilelab.datamesh.airbytespecificprovisioner.system.ApplicationConfiguration
import it.agilelab.datamesh.airbytespecificprovisioner.system.ApplicationConfiguration.httpPort
import it.agilelab.datamesh.airbytespecificprovisioner.validation.AirbyteValidator

import scala.jdk.CollectionConverters._

object Main extends LazyLogging {

  def run(port: Int, impl: ActorSystem[_] => SpecificProvisionerApiService): ActorSystem[Nothing] =
    ActorSystem[Nothing](
      Behaviors.setup[Nothing] { context =>
        import akka.actor.typed.scaladsl.adapter._
        implicit val classicSystem: actor.ActorSystem = context.system.toClassic

        val api = new SpecificProvisionerApi(
          impl(context.system),
          new ProvisionerApiMarshallerImpl(),
          SecurityDirectives.authenticateBasic("SecurityRealm", (_: Credentials) => Some(Seq.empty[(String, String)]))
        )

        val controller = new Controller(
          api,
          validationExceptionToRoute = Some { e =>
            logger.error("Error: ", e)
            val results = e.results()
            if (Option(results).isDefined) {
              results.crumbs().asScala.foreach(crumb => logger.info(crumb.crumb()))
              results.items().asScala.foreach { item =>
                logger.info(item.dataCrumbs())
                logger.info(item.dataJsonPointer())
                logger.info(item.schemaCrumbs())
                logger.info(item.message())
                logger.info("Severity: ", item.severity.getValue)
              }
              val message = e.results().items().asScala.map(_.message()).mkString("\n")
              complete((400, message))
            } else complete((400, e.getMessage))
          }
        )

        val _ = Http().newServerAt("0.0.0.0", port).bind(controller.routes)
        Behaviors.empty
      },
      BuildInfo.name.replaceAll("""\.""", "-")
    )

  def main(args: Array[String]): Unit = { val _ = run(httpPort, createImpl) }

  def createImpl(system: ActorSystem[_]): SpecificProvisionerApiService = {
    val syncProvision = new AirbyteWorkloadManager(new AirbyteValidator(), new AirbyteClient(system))
    if (ApplicationConfiguration.asyncConfiguration.provisionEnabled) {
      logger.info("Initializing provisioner in async provisioning mode")
      val repository = TaskRepository.fromConfig(ApplicationConfiguration.asyncConfiguration)
      new ProvisionerApiServiceImpl(
        new AsyncAirbyteWorkloadManager(syncProvision, repository, system.executionContext),
        new StatusService(repository)
      )
    } else {
      logger.info("Initializing provisioner in sync provisioning mode")
      new ProvisionerApiServiceImpl(syncProvision, GetStatus.alwaysCompletedStatus)
    }
  }
}
