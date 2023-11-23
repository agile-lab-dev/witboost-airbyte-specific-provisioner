package it.agilelab.datamesh.airbytespecificprovisioner.model

import io.circe.Json

case class AirbyteConnection(name: String, raw: Json)
