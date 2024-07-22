package it.agilelab.datamesh.airbytespecificprovisioner.api.intepreter

import akka.http.scaladsl.marshalling.Marshaller.stringMarshaller
import akka.http.scaladsl.marshalling.ToEntityMarshaller
import akka.http.scaladsl.model.MediaTypes._
import akka.http.scaladsl.server.Route
import cats.data.Validated.{Invalid, Valid}
import com.typesafe.scalalogging.LazyLogging
import it.agilelab.datamesh.airbytespecificprovisioner.api.SpecificProvisionerApiService
import it.agilelab.datamesh.airbytespecificprovisioner.integrator.WorkloadManager
import it.agilelab.datamesh.airbytespecificprovisioner.model._
import it.agilelab.datamesh.airbytespecificprovisioner.status.GetStatus

class ProvisionerApiServiceImpl(airbyteWorkloadManager: WorkloadManager, checkStatus: GetStatus)
    extends SpecificProvisionerApiService with LazyLogging {

  private val NotImplementedError = SystemError(
    error = "Endpoint not implemented",
    userMessage = Some("The requested feature hasn't been implemented"),
    input = None,
    inputErrorField = None,
    moreInfo = Some(ErrorMoreInfo(problems = List("Endpoint not implemented"), solutions = List.empty))
  )

  /** Code: 200, Message: The request status, DataType: Status
   *  Code: 400, Message: Invalid input, DataType: ValidationError
   *  Code: 500, Message: System problem, DataType: SystemError
   */
  override def getStatus(token: String)(implicit
      contexts: Seq[(String, String)],
      toEntityMarshallerValidationError: ToEntityMarshaller[RequestValidationError],
      toEntityMarshallerSystemError: ToEntityMarshaller[SystemError],
      toEntityMarshallerProvisioningStatus: ToEntityMarshaller[ProvisioningStatus]
  ): Route = checkStatus.statusOf(ComponentToken(token)).fold {
    val error = s"Couldn't find operation for token '$token'"
    getStatus400(RequestValidationError(
      errors = List(error),
      moreInfo = Some(ErrorMoreInfo(
        problems = List(error),
        solutions = List("Please try the operation again. If the problem subsists contact the platform team")
      )),
      userMessage = Some("It seems something went wrong while processing the task")
    ))
  }(result => getStatus200(ProvisioningStatusMapper.from(result)))

  /** Code: 200, Message: It synchronously returns the request result, DataType: ProvisioningStatus
   *  Code: 202, Message: If successful returns a provisioning deployment task token that can be used for polling the request status, DataType: String
   *  Code: 400, Message: Invalid input, DataType: ValidationError
   *  Code: 500, Message: System problem, DataType: SystemError
   */
  override def provision(provisioningRequest: ProvisioningRequest)(implicit
      contexts: Seq[(String, String)],
      toEntityMarshallerValidationError: ToEntityMarshaller[RequestValidationError],
      toEntityMarshallerSystemError: ToEntityMarshaller[SystemError],
      toEntityMarshallerProvisioningStatus: ToEntityMarshaller[ProvisioningStatus]
  ): Route =
    try airbyteWorkloadManager.provision(provisioningRequest.descriptorKind, provisioningRequest.descriptor) match {
        case Valid(e)   =>
          if (e.provisioningStatus.equals(ProvisionStatus.Running)) {
            // Marshalling to text/plain since coordinator actually expects the token that way
            provision202(e.componentToken.asString)(stringMarshaller(`text/plain`))
          } else provision200(ProvisioningStatusMapper.from(e))
        case Invalid(e) => provision400(ModelConverter.buildRequestValidationError(e))
      }
    catch {
      case t: Throwable =>
        logger.error(s"Exception in provision", t)
        provision500(ModelConverter.buildSystemError(t))
    }

  /** Code: 200, Message: It synchronously returns the request result, DataType: String
   *  Code: 400, Message: Invalid input, DataType: ValidationError
   *  Code: 500, Message: System problem, DataType: SystemError
   */
  override def validate(provisioningRequest: ProvisioningRequest)(implicit
      contexts: Seq[(String, String)],
      toEntityMarshallerSystemError: ToEntityMarshaller[SystemError],
      toEntityMarshallerValidationResult: ToEntityMarshaller[ValidationResult]
  ): Route =
    try airbyteWorkloadManager.validate(provisioningRequest.descriptorKind, provisioningRequest.descriptor) match {
        case Valid(_)   => validate200(ValidationResult(valid = true))
        case Invalid(e) =>
          val errors = e.map(_.errorMessage).toList
          validate200(ValidationResult(valid = false, error = Some(ValidationError(errors))))
      }
    catch {
      case t: Throwable =>
        logger.error(s"Exception in validate", t)
        validate500(ModelConverter.buildSystemError(t))
    }

  /** Code: 200, Message: It synchronously returns the request result, DataType: ProvisioningStatus
   *  Code: 202, Message: If successful returns a provisioning deployment task token that can be used for polling the request status, DataType: String
   *  Code: 400, Message: Invalid input, DataType: ValidationError
   *  Code: 500, Message: System problem, DataType: SystemError
   */
  override def unprovision(provisioningRequest: ProvisioningRequest)(implicit
      contexts: Seq[(String, String)],
      toEntityMarshallerValidationError: ToEntityMarshaller[RequestValidationError],
      toEntityMarshallerSystemError: ToEntityMarshaller[SystemError],
      toEntityMarshallerProvisioningStatus: ToEntityMarshaller[ProvisioningStatus]
  ): Route =
    try airbyteWorkloadManager.unprovision(provisioningRequest.descriptorKind, provisioningRequest.descriptor) match {
        case Valid(e)   =>
          if (e.provisioningStatus.equals(ProvisionStatus.Running))
            // Marshalling to text/plain since coordinator actually expects the token that way
            provision202(e.componentToken.asString)(stringMarshaller(`text/plain`))
          else provision200(ProvisioningStatusMapper.from(e))
        case Invalid(e) => unprovision400(ModelConverter.buildRequestValidationError(e))
      }
    catch {
      case t: Throwable =>
        logger.error(s"Exception in unprovision", t)
        unprovision500(ModelConverter.buildSystemError(t))
    }

  /** Code: 200, Message: It synchronously returns the access request response, DataType: ProvisioningStatus
   *  Code: 202, Message: It synchronously returns the access request response, DataType: String
   *  Code: 400, Message: Invalid input, DataType: ValidationError
   *  Code: 500, Message: System problem, DataType: SystemError
   */
  override def updateacl(updateAclRequest: UpdateAclRequest)(implicit
      contexts: Seq[(String, String)],
      toEntityMarshallerValidationError: ToEntityMarshaller[RequestValidationError],
      toEntityMarshallerSystemError: ToEntityMarshaller[SystemError],
      toEntityMarshallerProvisioningStatus: ToEntityMarshaller[ProvisioningStatus]
  ): Route = updateacl500(NotImplementedError)

  /** Code: 202, Message: It returns a token that can be used for polling the async validation operation status and results, DataType: String
   *  Code: 400, Message: Invalid input, DataType: RequestValidationError
   *  Code: 500, Message: System problem, DataType: SystemError
   */
  override def asyncValidate(provisioningRequest: ProvisioningRequest)(implicit
      contexts: Seq[(String, String)],
      toEntityMarshallerRequestValidationError: ToEntityMarshaller[RequestValidationError],
      toEntityMarshallerSystemError: ToEntityMarshaller[SystemError]
  ): Route = asyncValidate500(NotImplementedError)

  /** Code: 200, Message: The request status and results, DataType: ReverseProvisioningStatus
   *  Code: 400, Message: Invalid input, DataType: RequestValidationError
   *  Code: 500, Message: System problem, DataType: SystemError
   */
  override def getReverseProvisioningStatus(token: String)(implicit
      contexts: Seq[(String, String)],
      toEntityMarshallerRequestValidationError: ToEntityMarshaller[RequestValidationError],
      toEntityMarshallerSystemError: ToEntityMarshaller[SystemError],
      toEntityMarshallerReverseProvisioningStatus: ToEntityMarshaller[ReverseProvisioningStatus]
  ): Route = getReverseProvisioningStatus500(NotImplementedError)

  /** Code: 200, Message: The request status, DataType: ValidationStatus
   *  Code: 400, Message: Invalid input, DataType: RequestValidationError
   *  Code: 500, Message: System problem, DataType: SystemError
   */
  override def getValidationStatus(token: String)(implicit
      contexts: Seq[(String, String)],
      toEntityMarshallerRequestValidationError: ToEntityMarshaller[RequestValidationError],
      toEntityMarshallerSystemError: ToEntityMarshaller[SystemError],
      toEntityMarshallerValidationStatus: ToEntityMarshaller[ValidationStatus]
  ): Route = getValidationStatus500(NotImplementedError)

  /** Code: 200, Message: It synchronously returns the reverse provisioning response, DataType: ReverseProvisioningStatus
   *  Code: 202, Message: It returns a reverse provisioning task token that can be used for polling the request status, DataType: String
   *  Code: 400, Message: Invalid input, DataType: RequestValidationError
   *  Code: 500, Message: System problem, DataType: SystemError
   */
  override def runReverseProvisioning(reverseProvisioningRequest: ReverseProvisioningRequest)(implicit
      contexts: Seq[(String, String)],
      toEntityMarshallerRequestValidationError: ToEntityMarshaller[RequestValidationError],
      toEntityMarshallerSystemError: ToEntityMarshaller[SystemError],
      toEntityMarshallerReverseProvisioningStatus: ToEntityMarshaller[ReverseProvisioningStatus]
  ): Route = runReverseProvisioning500(NotImplementedError)
}
