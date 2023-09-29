package it.agilelab.datamesh.airbytespecificprovisioner.api.intepreter

import akka.http.scaladsl.marshalling.ToEntityMarshaller
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.unmarshalling.FromEntityUnmarshaller
import de.heikoseeberger.akkahttpcirce.FailFastCirceSupport.{marshaller, unmarshaller}
import it.agilelab.datamesh.airbytespecificprovisioner.api.SpecificProvisionerApiService
import it.agilelab.datamesh.airbytespecificprovisioner.integrator.AirbyteWorkloadManager
import it.agilelab.datamesh.airbytespecificprovisioner.model._

class ProvisionerApiServiceImpl(airbyteWorkloadManager: AirbyteWorkloadManager) extends SpecificProvisionerApiService {

  // Json String
  implicit val toEntityMarshallerJsonString: ToEntityMarshaller[String]       = marshaller[String]
  implicit val toEntityUnmarshallerJsonString: FromEntityUnmarshaller[String] = unmarshaller[String]

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
  ): Route = {
    val error = "Asynchronous task provisioning is not yet implemented"
    getStatus400(RequestValidationError(
      errors = List(error),
      userMessage = Some(error),
      input = Some(token),
      inputErrorField = None,
      moreInfo = Some(ErrorMoreInfo(problems = List(error), List.empty))
    ))
  }

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
    airbyteWorkloadManager.provision(provisioningRequest.descriptorKind, provisioningRequest.descriptor) match {
      case Left(err)  => err match {
          case e: ValidationError => provision400(ModelConverter.buildRequestValidationError(e))
          case e: SystemError     => provision500(e)
        }
      case Right(res) => provision200(ProvisioningStatus(ProvisioningStatusEnums.StatusEnum.COMPLETED, res))
      case _          => provision500(SystemError("generic error"))
    }

  /** Code: 200, Message: It synchronously returns the request result, DataType: String
   *  Code: 400, Message: Invalid input, DataType: ValidationError
   *  Code: 500, Message: System problem, DataType: SystemError
   */
  override def validate(provisioningRequest: ProvisioningRequest)(implicit
      contexts: Seq[(String, String)],
      toEntityMarshallerSystemError: ToEntityMarshaller[SystemError],
      toEntityMarshallerValidationResult: ToEntityMarshaller[ValidationResult]
  ): Route = validate200(ValidationResult(valid = true))

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
    airbyteWorkloadManager.unprovision(provisioningRequest.descriptorKind, provisioningRequest.descriptor) match {
      case Left(err)  => err match {
          case e: ValidationError => unprovision400(ModelConverter.buildRequestValidationError(e))
          case e: SystemError     => unprovision500(e)
        }
      case Right(res) => unprovision200(ProvisioningStatus(ProvisioningStatusEnums.StatusEnum.COMPLETED, res))
      case _          => unprovision500(SystemError("generic error"))
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
  ): Route = updateacl200(ProvisioningStatus(ProvisioningStatusEnums.StatusEnum.COMPLETED, "OK"))

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
