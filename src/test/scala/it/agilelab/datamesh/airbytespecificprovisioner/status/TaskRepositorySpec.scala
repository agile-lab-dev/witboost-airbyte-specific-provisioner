package it.agilelab.datamesh.airbytespecificprovisioner.status

import it.agilelab.datamesh.airbytespecificprovisioner.system.ApplicationConfiguration.AsyncConfiguration
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

class TaskRepositorySpec extends AnyFlatSpec with should.Matchers {

  "apply method" should "create CacheRepository" in {
    val asyncConfig = AsyncConfiguration("cache", provisionEnabled = true)
    TaskRepository.fromConfig(asyncConfig) shouldBe a[CacheRepository]
  }

}
