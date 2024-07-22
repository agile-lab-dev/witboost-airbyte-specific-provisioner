package it.agilelab.datamesh.airbytespecificprovisioner.integrator

import cats.data.ValidatedNel
import it.agilelab.datamesh.airbytespecificprovisioner.error.{ErrorType, ValidationErrorType}
import it.agilelab.datamesh.airbytespecificprovisioner.model.{AirbyteFields, DescriptorKind, ProvisionResult}

trait WorkloadManager {

  def validate(descriptorKind: DescriptorKind, descriptor: String): ValidatedNel[ValidationErrorType, AirbyteFields]

  def provision(descriptorKind: DescriptorKind, descriptor: String): ValidatedNel[ErrorType, ProvisionResult]

  def unprovision(descriptorKind: DescriptorKind, descriptor: String): ValidatedNel[ErrorType, ProvisionResult]
}
