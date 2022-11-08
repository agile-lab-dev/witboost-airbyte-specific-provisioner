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

  case class SnowflakeConfiguration(user: String, password: String, role: String)

  def snowflakeConfiguration: SnowflakeConfiguration = SnowflakeConfiguration(
    user = config.get.getString("snowflake.user"),
    password = config.get.getString("snowflake.password"),
    role = config.get.getString("snowflake.role")
  )

  case class AirbyteConfiguration(
      invocationTimeout: Int,
      baseUrl: String,
      workspaceId: String,
      sourceId: String,
      destinationId: String
  )

  def airbyteConfiguration: AirbyteConfiguration = AirbyteConfiguration(
    invocationTimeout = config.get.getInt("airbyte.invocation-timeout"),
    baseUrl = config.get.getString("airbyte.base-url"),
    workspaceId = config.get.getString("airbyte.workspace-id"),
    sourceId = config.get.getString("airbyte.source-id"),
    destinationId = config.get.getString("airbyte.destination-id")
  )

}
