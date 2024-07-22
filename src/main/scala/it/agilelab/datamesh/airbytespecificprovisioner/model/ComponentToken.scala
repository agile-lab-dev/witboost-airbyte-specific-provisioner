package it.agilelab.datamesh.airbytespecificprovisioner.model

object ComponentToken {
  def empty: ComponentToken = ComponentToken("")
}

/** A string that identifies the resources for which a provisioning
 *  request has been accepted to be performed in an asynchronous fashion.
 *
 *  The token should contain enough information to retrieve the status
 *  of the resources.
 *
 *  As a client of the framework you are free to structure the token
 *  at your will.
 *
 *  @param string: an identifier of the provisioning request
 */
case class ComponentToken(string: String) {

  /** @return the token value as type string */
  def asString: String = string

  /** @return true if the identifier is empty */
  def isEmpty: Boolean = string.isEmpty
}
