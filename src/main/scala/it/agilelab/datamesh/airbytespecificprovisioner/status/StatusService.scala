package it.agilelab.datamesh.airbytespecificprovisioner.status

import com.typesafe.scalalogging.StrictLogging
import it.agilelab.datamesh.airbytespecificprovisioner.model.{ComponentToken, ProvisionResult}

class StatusService(taskRepository: TaskRepository) extends GetStatus with StrictLogging {

  override def statusOf(token: ComponentToken): Option[ProvisionResult] = {
    logger.info("Querying repository for task with token '{}'", token.asString)
    val maybeTask = taskRepository.findTask(token).map(_.toProvisionResult)
    maybeTask match {
      case Some(ProvisionResult(status, _, _, _)) => logger
          .info("Task with token '{}' found with status {}", token.asString, status)
      case None => logger.warn("Task with token '{}' doesn't exist on repository", token.asString)
    }
    maybeTask
  }
}
