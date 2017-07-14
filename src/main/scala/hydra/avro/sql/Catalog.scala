package hydra.avro.sql

import scala.util.Try

abstract class Catalog {

  def createTable(tableDesc: Table): Boolean

  def createSchema(schema: String): Boolean

  def tableExists(tableIdentifier: TableIdentifier): Boolean

  def schemaExists(schema: String): Boolean

  def getTable(tableIdentifier: TableIdentifier): Try[Table]

}
