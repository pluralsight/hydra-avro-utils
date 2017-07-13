package hydra.avro.sql

import scala.util.Try

abstract class Catalog {

  def createTable(tableDesc: Table): Table

  def tableExists(tableIdentifier: TableIdentifier): Boolean

  def getTable(tableIdentifier: TableIdentifier): Try[Table]

}
