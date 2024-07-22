package it.agilelab.datamesh.airbytespecificprovisioner.status

import it.agilelab.datamesh.airbytespecificprovisioner.model.ComponentToken
import it.agilelab.datamesh.airbytespecificprovisioner.system.ApplicationConfiguration.AsyncConfiguration
import org.slf4j.{Logger, LoggerFactory}

trait TaskRepository {

  /** Saves a task to be retrieved at a later moment in time */
  def createTask(task: Task): Task

  /** Queries a task using token as the identifier */
  def findTask(token: ComponentToken): Option[Task]

  /** Updates an existing task identified by the taskId with the one received */
  def updateTask(task: Task): Task

  /** Deletes a task using the taskId as the identifier */
  def deleteTask(token: ComponentToken): Option[Task]

  /** Forces FAILED status on all tasks created by a specific provisioner instance.
   *  This is done to avoid leaving hanging tasks when one instance crashes or goes down.
   *
   *  Currently forceFail fails all task, as provisionerId is not yet implemented
   */
  def forceFail(): List[Task]
}

object TaskRepository {
  private val logger: Logger = LoggerFactory.getLogger(getClass.getName)

  val CACHE_REPOSITORY_TYPE = "cache"

  def fromConfig(repositoryConfig: AsyncConfiguration): TaskRepository = repositoryConfig.repositoryType match {
    case CACHE_REPOSITORY_TYPE => new CacheRepository
    case _                     =>
      logger.warn(s"No configuration found for async repository type, fall-backing to Cache Repository")
      new CacheRepository // TODO currently supporting only CacheRepository
  }
}
