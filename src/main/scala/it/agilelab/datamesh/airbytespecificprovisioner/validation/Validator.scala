package it.agilelab.datamesh.airbytespecificprovisioner.validation

import cats.data.ValidatedNel
import it.agilelab.datamesh.airbytespecificprovisioner.error.ValidationErrorType
import it.agilelab.datamesh.airbytespecificprovisioner.model.{AirbyteFields, DescriptorKind}

trait Validator {
  def validate(descriptorKind: DescriptorKind, descriptor: String): ValidatedNel[ValidationErrorType, AirbyteFields]
}
