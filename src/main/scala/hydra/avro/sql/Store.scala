package hydra.avro.sql

import org.apache.avro.generic.GenericRecord

import scala.util.Try

abstract class Store {

  def createTable(tableDesc: Table): Table

  def tableExists(tableIdentifier: TableIdentifier): Boolean

  def insertRecord(record: GenericRecord, table: Table)

  def getTable(tableIdentifier: TableIdentifier): Try[Table]

  def close():Unit
}
