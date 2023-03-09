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

  case class SnowflakeConfiguration(host: String, user: String, password: String, role: String, warehouse: String)

  def snowflakeConfiguration: SnowflakeConfiguration = SnowflakeConfiguration(
    host = config.get.getString("snowflake.host"),
    user = config.get.getString("snowflake.user"),
    password = config.get.getString("snowflake.password"),
    role = config.get.getString("snowflake.role"),
    warehouse = config.get.getString("snowflake.warehouse")
  )

  case class AirbyteConfiguration(
      invocationTimeout: Int,
      baseUrl: String,
      workspaceId: String,
      sourceId: String,
      destinationId: String,
      dbtGitToken: String,
      dbtGitUser: String,
      airbyteUser: String,
      airbytePassword: String,
      basicAuth: String
  )

  def airbyteConfiguration: AirbyteConfiguration = AirbyteConfiguration(
    invocationTimeout = config.get.getInt("airbyte.invocation-timeout"),
    baseUrl = config.get.getString("airbyte.base-url"),
    workspaceId = config.get.getString("airbyte.workspace-id"),
    sourceId = config.get.getString("airbyte.source-id"),
    destinationId = config.get.getString("airbyte.destination-id"),
    dbtGitToken = config.get.getString("airbyte.git-token"),
    dbtGitUser = config.get.getString("airbyte.dbt-user"),
    airbyteUser = config.get.getString("airbyte.airbyte-user"),
    airbytePassword = config.get.getString("airbyte.airbyte-password"),
    basicAuth = config.get.getString("airbyte.basic-auth")
  )

}
