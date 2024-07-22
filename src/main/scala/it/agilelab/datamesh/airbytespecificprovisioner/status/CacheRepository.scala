package it.agilelab.datamesh.airbytespecificprovisioner.status

import it.agilelab.datamesh.airbytespecificprovisioner.model.ComponentToken
import org.slf4j.{Logger, LoggerFactory}

import java.util.concurrent.ConcurrentHashMap
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class CacheRepository extends TaskRepository {

  private val logger: Logger = LoggerFactory.getLogger(getClass.getName)

  private val tasks: ConcurrentHashMap[ComponentToken, Task] = new ConcurrentHashMap()

  override def createTask(task: Task): Task = Option(tasks.putIfAbsent(task.id, task)).getOrElse(task)

  override def findTask(token: ComponentToken): Option[Task] = {
    val task = Option(tasks.get(token))
    logger.debug("Retrieved task {} with token '{}'", task, token)
    task
  }

  override def updateTask(task: Task): Task = {
    tasks.put(task.id, task)
    tasks.get(task.id)
  }

  override def deleteTask(token: ComponentToken): Option[Task] = Option(tasks.remove(token))

  override def forceFail(): List[Task] = {
    val changed: mutable.ListBuffer[Task] = ListBuffer()
    tasks.forEach { (key, value) =>
      if (!TaskStatus.hasTerminated(value.status)) {
        val newTask = value.copy(status = TaskStatus.FAILED)
        tasks.replace(key, newTask)
        changed.append(newTask)
      }
    }
    changed.toList
  }
}
