package it.agilelab.datamesh.airbytespecificprovisioner.model

import io.circe.Json

case class AirbyteDestination(name: String, connectionConfiguration: Json, raw: Json)
