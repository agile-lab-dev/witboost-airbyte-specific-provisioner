package it.agilelab.datamesh.airbytespecificprovisioner.system

import com.typesafe.config.{Config, ConfigFactory, ConfigRenderOptions}

import java.util.concurrent.atomic.AtomicReference
import scala.annotation.tailrec

object ApplicationConfiguration {

  val config: AtomicReference[Config] = new AtomicReference(ConfigFactory.load())

  def reloadConfig(): String = config.synchronized {
    @tailrec
    def snip(): Unit = {
      val oldConf = config.get
      val newConf = {
        ConfigFactory.invalidateCaches()
        ConfigFactory.load()
      }
      if (!config.compareAndSet(oldConf, newConf)) snip()
    }
    snip()
    config.get.getObject("specific-provisioner").render(ConfigRenderOptions.defaults())
  }

  def httpPort: Int = config.get.getInt("specific-provisioner.http-port")

  def airbyteInvocationTimeout = config.get.getInt("specific-provisioner.airbyte.invocation-timeout")

  def airbyteBaseUrl: String = config.get.getString("specific-provisioner.airbyte.base-url")

  def airbyteSourcesCreationEndpoint: String = config.get
    .getString("specific-provisioner.airbyte.sources-creation-endpoint")

  def airbyteDestinationsCreationEndpoint: String = config.get
    .getString("specific-provisioner.airbyte.destinations-creation-endpoint")

  def airbyteConnectionsCreationEndpoint: String = config.get
    .getString("specific-provisioner.airbyte.connections-creation-endpoint")

}
