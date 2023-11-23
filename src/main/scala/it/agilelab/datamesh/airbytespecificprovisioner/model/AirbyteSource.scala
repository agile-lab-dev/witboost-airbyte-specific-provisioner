package it.agilelab.datamesh.airbytespecificprovisioner.model

import io.circe.Json

case class AirbyteSource(name: String, connectionConfiguration: Json, raw: Json)
