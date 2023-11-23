package it.agilelab.datamesh.airbytespecificprovisioner.model

case class AirbyteFields(
    dpFields: DataProductFields,
    source: AirbyteSource,
    destination: AirbyteDestination,
    connection: AirbyteConnection
)
