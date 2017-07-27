package hydra.avro.sql

import java.sql.{BatchUpdateException, PreparedStatement}

import com.google.common.cache.{RemovalListener, RemovalNotification}
import com.zaxxer.hikari.HikariDataSource
import hydra.avro.io.SaveMode.SaveMode
import hydra.avro.io.{RecordWriter, SaveMode}
import io.confluent.kafka.schemaregistry.avro.AvroCompatibilityChecker
import io.confluent.kafka.schemaregistry.avro.AvroCompatibilityChecker.NO_OP_CHECKER
import org.apache.avro.generic.GenericRecord
import org.apache.avro.{AvroRuntimeException, Schema}
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.util.{Failure, Success}


/**
  * Created by alexsilva on 7/11/17.
  *
  * A batch size of 0 means that this class will never do any executeBatch and that external clients need to call
  * flush()
  *
  * If the primary keys are provided as a constructor argument, it overrides anything that
  * may have been provided by the schema.
  */
class JdbcRecordWriter(val dataSource: HikariDataSource,
                       val schema: Schema,
                       val mode: SaveMode = SaveMode.ErrorIfExists,
                       dialect: JdbcDialect,
                       override val compatibilityChecker: AvroCompatibilityChecker = NO_OP_CHECKER,
                       dbSyntax: DbSyntax = UnderscoreSyntax,
                       batchSize: Int = 50,
                       table: Option[String] = None,
                       database: Option[String] = None) extends RecordWriter with JdbcHelper {

  private val store: Catalog = new JdbcCatalog(dataSource, dbSyntax, dialect)

  private val tableName = table.getOrElse(schema.getName)

  private val tableId = TableIdentifier(tableName, database)

  private val valueSetter = new AvroValueSetter(schema, dialect, dbSyntax)

  private val unflushedRecords = new mutable.ArrayBuffer[GenericRecord]()

  private var rowCount = 0L

  private val tableObj: Table = {
    val catalogTable = store.getTable(tableId).map { table =>
      mode match {
        case SaveMode.ErrorIfExists => throw new AnalysisException(s"Table $table already exists.")
        case SaveMode.Overwrite => //todo: wipeout table
          table
        case _ => table
      }
    }.recover {
      case _: NoSuchTableException =>
        val table = Table(tableName, schema, database)
        store.createTable(table)
        table
      case e: Throwable => {
        throw e
      }
    }

    catalogTable match {
      case Success(table) => table
      case Failure(ex) => throw ex;
    }
  }

  // private val constPk: Option[Seq[Schema.Field]] = primaryKeys.map(_.split(",").map(schema.getField))
  // private val pk: Seq[Schema.Field] = constPk.getOrElse(JdbcUtils.getIdFields(schema))
  private val pk = JdbcUtils.getIdFields(schema)
  private val name = tableObj.dbSchema.map(_ + ".").getOrElse("") + dbSyntax.format(tableObj.name)
  private val stmt = dialect.upsert(dbSyntax.format(name), schema, dbSyntax, pk)

  def write(record: GenericRecord): Unit = {
    if (!compatibilityChecker.isCompatible(schema, record.getSchema)) {
      throw new AvroRuntimeException("Schemas are not compatible.")
    }

    unflushedRecords += record
    rowCount += 1
    if (batchSize > 0 && rowCount % batchSize == 0) {
      flush()
      rowCount = 0
    }
  }

  def flush(): Unit = synchronized {
    withConnection(dataSource.getConnection) { conn =>
      val pstmt = conn.prepareStatement(stmt)
      unflushedRecords.foreach { r =>
        valueSetter.setValues(r, pstmt)
        pstmt.addBatch()
      }
      pstmt.executeBatch()
      unflushedRecords.clear()
    }
  }

  def close(): Unit = {
    flush()
  }


  private def executeBatch(stmt: PreparedStatement) = {
    try {
      stmt.executeBatch()
    }
    catch {
      case e: BatchUpdateException => e.getNextException().printStackTrace()
    }
  }
}

object JdbcRecordWriter {
  val logger = LoggerFactory.getLogger(getClass)

  val removalListener = new RemovalListener[String, PreparedStatement] {
    override def onRemoval(n: RemovalNotification[String, PreparedStatement]): Unit = {
      logger.debug(s"Entry ${n.getKey} was removed. Closing and flushing prepared statement.")
      try {
        n.getValue.executeBatch()
      }
      finally {
        n.getValue.close()
      }
    }
  }
}