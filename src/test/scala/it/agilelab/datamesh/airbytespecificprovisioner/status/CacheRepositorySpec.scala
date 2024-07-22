package it.agilelab.datamesh.airbytespecificprovisioner.status

import it.agilelab.datamesh.airbytespecificprovisioner.model.ComponentToken
import it.agilelab.datamesh.airbytespecificprovisioner.status.TaskOperation.{PROVISION, VALIDATE}
import it.agilelab.datamesh.airbytespecificprovisioner.status.TaskStatus.{COMPLETED, FAILED, RUNNING, WAITING}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

class CacheRepositorySpec extends AnyFlatSpec with should.Matchers {

  "CacheRepository" should "forceFail" in {
    val repo = new CacheRepository

    fill(
      repo,
      List(
        Task(ComponentToken("id1"), COMPLETED, PROVISION, Info()),
        Task(ComponentToken("id2"), RUNNING, PROVISION, Info()),
        Task(ComponentToken("id3"), WAITING, PROVISION, Info())
      )
    )

    val failedTasks = repo.forceFail()

    failedTasks.length shouldEqual 2
    failedTasks.map(_.id.asString) should contain theSameElementsAs List("id2", "id3")
    failedTasks.foreach(_.status shouldEqual FAILED)

  }

  it should "updateTask" in {
    val repo        = new CacheRepository
    val toBeUpdated = Task(ComponentToken("id2"), RUNNING, PROVISION, Info())
    fill(
      repo,
      List(
        Task(ComponentToken("id1"), COMPLETED, PROVISION, Info()),
        toBeUpdated,
        Task(ComponentToken("id3"), WAITING, PROVISION, Info())
      )
    )

    val updatedTask = repo.updateTask(toBeUpdated.copy(status = FAILED))

    updatedTask shouldEqual toBeUpdated.copy(status = FAILED)
  }

  it should "findTask" in {
    val repo = new CacheRepository

    val id        = ComponentToken("id2")
    val toBeFound = Task(id, RUNNING, PROVISION, Info())

    fill(
      repo,
      List(
        Task(ComponentToken("id1"), COMPLETED, PROVISION, Info()),
        toBeFound,
        Task(ComponentToken("id3"), WAITING, PROVISION, Info())
      )
    )
    val maybeTask = repo.findTask(id)

    maybeTask shouldNot be(None)
    maybeTask.get shouldEqual toBeFound

  }

  it should "return None on find non-existing Task" in {
    val repo = new CacheRepository

    val id = ComponentToken("id4")

    fill(
      repo,
      List(
        Task(ComponentToken("id1"), COMPLETED, PROVISION, Info()),
        Task(ComponentToken("id2"), RUNNING, PROVISION, Info()),
        Task(ComponentToken("id3"), WAITING, PROVISION, Info())
      )
    )
    val maybeTask = repo.findTask(id)

    maybeTask shouldBe None
  }

  it should "createTask" in {
    val repo = new CacheRepository

    val toBeCreated = Task(ComponentToken("id4"), COMPLETED, VALIDATE, Info())

    fill(
      repo,
      List(
        Task(ComponentToken("id1"), COMPLETED, PROVISION, Info()),
        Task(ComponentToken("id2"), RUNNING, PROVISION, Info()),
        Task(ComponentToken("id3"), WAITING, PROVISION, Info())
      )
    )

    val newTask = repo.createTask(toBeCreated)

    newTask shouldEqual toBeCreated
  }

  it should "return stored task if already existent on createTask" in {
    val repo = new CacheRepository

    val toBeCreated  = Task(ComponentToken("id1"), RUNNING, VALIDATE, Info())
    val existingTask = Task(ComponentToken("id1"), COMPLETED, PROVISION, Info())

    fill(
      repo,
      List(
        existingTask,
        Task(ComponentToken("id2"), RUNNING, PROVISION, Info()),
        Task(ComponentToken("id3"), WAITING, PROVISION, Info())
      )
    )

    val newTask = repo.createTask(toBeCreated)

    newTask shouldNot equal(toBeCreated)
    newTask shouldEqual existingTask
  }

  it should "create existing task" in {
    val repo = new CacheRepository

    val toBeCreated = Task(ComponentToken("id4"), COMPLETED, VALIDATE, Info())

    fill(
      repo,
      List(
        Task(ComponentToken("id1"), COMPLETED, PROVISION, Info()),
        Task(ComponentToken("id2"), RUNNING, PROVISION, Info()),
        Task(ComponentToken("id3"), WAITING, PROVISION, Info())
      )
    )

    val newTask = repo.createTask(toBeCreated)

    newTask shouldEqual toBeCreated
  }

  it should "delete existing Task" in {
    val repo = new CacheRepository

    val id          = ComponentToken("id2")
    val toBeDeleted = Task(id, RUNNING, PROVISION, Info())

    fill(
      repo,
      List(
        Task(ComponentToken("id1"), COMPLETED, PROVISION, Info()),
        toBeDeleted,
        Task(ComponentToken("id3"), WAITING, PROVISION, Info())
      )
    )

    val deletedTask = repo.deleteTask(id)

    deletedTask shouldNot be(None)
    deletedTask.get shouldEqual toBeDeleted
  }

  it should "return None on deleting non-existing Task" in {
    val repo = new CacheRepository

    val id = ComponentToken("id4")

    fill(
      repo,
      List(
        Task(ComponentToken("id1"), COMPLETED, PROVISION, Info()),
        Task(ComponentToken("id2"), RUNNING, PROVISION, Info()),
        Task(ComponentToken("id3"), WAITING, PROVISION, Info())
      )
    )

    val deletedTask = repo.deleteTask(id)

    deletedTask shouldBe None
  }

  private def fill(repo: TaskRepository, tasks: Seq[Task]): Seq[Task] = tasks.map(task => repo.createTask(task))

}
