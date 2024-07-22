package it.agilelab.datamesh.airbytespecificprovisioner.integrator

import cats.data.Validated.{Invalid, Valid}
import cats.data.ValidatedNel
import com.typesafe.scalalogging.StrictLogging
import it.agilelab.datamesh.airbytespecificprovisioner.error.{AsyncProvisionFailure, ErrorType, ValidationErrorType}
import it.agilelab.datamesh.airbytespecificprovisioner.model.{
  AirbyteFields,
  ComponentToken,
  DescriptorKind,
  ProvisionResult
}
import it.agilelab.datamesh.airbytespecificprovisioner.status.TaskOperation.TaskOperation
import it.agilelab.datamesh.airbytespecificprovisioner.status._

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

class AsyncAirbyteWorkloadManager(
    syncWorkloadManager: WorkloadManager,
    taskRepository: TaskRepository,
    implicit val executionContext: ExecutionContext
) extends WorkloadManager with StrictLogging {

  private def buildTask(taskOperation: TaskOperation): Task =
    Task(ComponentToken(UUID.randomUUID().toString), TaskStatus.WAITING, taskOperation, Info())

  override def validate(
      descriptorKind: DescriptorKind,
      descriptor: String
  ): ValidatedNel[ValidationErrorType, AirbyteFields] = syncWorkloadManager.validate(descriptorKind, descriptor)

  override def provision(
      descriptorKind: DescriptorKind,
      descriptor: String
  ): ValidatedNel[ErrorType, ProvisionResult] = {
    val task = taskRepository.createTask(buildTask(TaskOperation.PROVISION))
    handleTask(task, syncWorkloadManager.provision(descriptorKind, descriptor))
    Valid(ProvisionResult.running(task.id))
  }

  override def unprovision(
      descriptorKind: DescriptorKind,
      descriptor: String
  ): ValidatedNel[ErrorType, ProvisionResult] = {
    val task = taskRepository.createTask(buildTask(TaskOperation.UNPROVISION))
    handleTask(task, syncWorkloadManager.unprovision(descriptorKind, descriptor))
    Valid(ProvisionResult.running(task.id))
  }

  private def handleTask(
      task: Task,
      blockingFunction: => ValidatedNel[ErrorType, ProvisionResult]
  ): Future[ValidatedNel[ErrorType, ProvisionResult]] = Future {
    logger.info(s"Starting execution of task ${task.id} ${task.operation}")
    taskRepository.updateTask(task.copy(status = TaskStatus.RUNNING))
    blockingFunction
  }.andThen { result =>
    logger.info(s"Async task ${task.id} ${task.operation} finished. Is successful? ${result.isSuccess}")
    taskRepository.findTask(task.id).map { updatedTask =>
      if (!TaskStatus.hasTerminated(updatedTask.status)) {
        logger.debug(s"Updating task $updatedTask with result $result")
        result match {
          case Failure(exception)              =>
            logger.error(s"Task ${task.id} ${task.operation} failed with exception", exception)
            taskRepository.updateTask(updatedTask.copy(
              status = TaskStatus.FAILED,
              info = Info(errors =
                List(AsyncProvisionFailure(s"Error while executing ${updatedTask.operation} task", exception))
              )
            ))
          case Success(Invalid(error))         => taskRepository
              .updateTask(updatedTask.copy(status = TaskStatus.FAILED, info = Info(errors = error.toList)))
          case Success(Valid(provisionResult)) => taskRepository
              .updateTask(Task.fromProvisionResult(provisionResult, updatedTask.operation).copy(id = task.id))
        }
      }
    }
  }
}
