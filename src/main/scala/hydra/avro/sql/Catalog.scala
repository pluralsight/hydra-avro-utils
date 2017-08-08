package hydra.avro.sql

import scala.util.Try

abstract class Catalog {

  def createOrAlterTable(tableDesc: Table): Boolean

  def createSchema(schema: String): Boolean

  def tableExists(tableIdentifier: TableIdentifier): Boolean

  def schemaExists(schema: String): Boolean

  def getTableMetadata(tableIdentifier: TableIdentifier): Try[DbTable]

}
