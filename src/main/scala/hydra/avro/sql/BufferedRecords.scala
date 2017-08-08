package hydra.avro.sql

import java.sql.{Connection, SQLException, Statement}
import java.util.Objects

import hydra.avro.util.AvroUtils
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.slf4j.LoggerFactory

import scala.collection.mutable

class BufferedRecords(schema: Schema, table: String, dialect: JdbcDialect, dbSyntax: DbSyntax, conn: Connection, batchSize: Int) {

  import BufferedRecords.logger

  private val isUpsert = !AvroUtils.getPrimaryKeys(schema).isEmpty
  private val stmt = conn.prepareStatement(dialect.upsert(table, schema, dbSyntax))
  private val binder = new AvroValueSetter(schema, dialect)
  private val records = new mutable.ArrayBuffer[GenericRecord]()

  def add(record: GenericRecord): Unit = {
    if (Objects.equals(schema, record.getSchema)) {
      records += record
      if (records.size >= batchSize) flush() else Seq.empty
    } else {
      // Each batch needs to have the same dbInfo, so get the buffered records out, reset state if possible,
      // add columns and re-attempt the add
      flush()
      throw new IllegalArgumentException("Not a valid record.")
    }
  }

  def flush() = {
    records.foreach(record => binder.bind(record, stmt))
    var totalUpdateCount = 0
    var successNoInfo = false
    stmt.executeBatch().foreach { count =>
      if (count == Statement.SUCCESS_NO_INFO) successNoInfo = true else totalUpdateCount += count
    }

    if (totalUpdateCount != records.size && !successNoInfo) {
      if (isUpsert) {
        logger.trace("inserted records:{} resulting in in totalUpdateCount:{}", records.size, totalUpdateCount)
      }
      else {
        val msg = s"Update count $totalUpdateCount did not sum up to total number of records inserted ${records.size}"
        throw new SQLException(msg)
      }

    }

    if (successNoInfo) {
      logger.info("inserted records:{} , but no count of the number of rows it affected is available", records.size)
    }
    val flushed = records.map(identity)(collection.breakOut)
    records.clear()
    flushed
  }

  def close(): Unit = {
    Option(stmt).foreach(_.close())
  }
}

object BufferedRecords {
  val logger = LoggerFactory.getLogger(getClass)
}


