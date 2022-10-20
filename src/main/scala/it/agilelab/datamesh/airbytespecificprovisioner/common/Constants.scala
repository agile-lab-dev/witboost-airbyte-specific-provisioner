package it.agilelab.datamesh.airbytespecificprovisioner.common

object Constants {

  val DATA_PRODUCT           = "dataProduct"
  val COMPONENT              = "component"
  val WORKLOAD               = "workload"
  val KIND                   = "kind"
  val DOMAIN                 = "domain"
  val NAME                   = "name"
  val ID                     = "id"
  val ENVIRONMENT            = "environment"
  val DATA_PRODUCT_OWNER     = "dataProductOwner"
  val OWNER_GROUP            = "ownerGroup"
  val SPECIFIC               = "specific"
  val PROVISIONING_RESULT    = "provisioningResult"
  val TYPE_NAME              = "typeName"
  val QUALIFIED_NAME         = "qualifiedName"
  val TAGS                   = "tags"
  val TAG_FQN                = "tagFQN"
  val EMAIL                  = "email"
  val VERSION                = "version"
  val DESCRIPTION            = "description"
  val DATA_SHARING_AGREEMENT = "dataSharingAgreement"
  val CREATION_DATE          = "creationDate"
  val DATA_CONTRACT          = "dataContract"
  val SCHEMA                 = "schema"
  val PROCESS_DESCRIPTION    = "processDescription"

  val SOURCE      = "source"
  val DESTINATION = "destination"
  val CONNECTION  = "connection"

  val CREATE_ACTION = "create"
  val LIST_ACTION   = "list"
  val DELETE_ACTION = "delete"

  val NOT_ALPHANUMERIC_NOR_UNDERSCORE_NOR_DASH = "[^a-zA-Z0-9_-]"
  val NOT_ALPHANUMERIC_NOR_DASH                = "[^a-zA-Z0-9-]"
  val PREFIX_WITH_COLON                        = "^[^:]+:"

  val DASH         = "-"
  val COMMA        = ","
  val UNDERSCORE   = "_"
  val AT           = "@"
  val EMPTY_STRING = ""
  val BLANK_SPACE  = " "
  val SLASH        = "/"

}
