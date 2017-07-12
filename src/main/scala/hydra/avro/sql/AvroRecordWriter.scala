package hydra.avro.sql

import com.typesafe.config.Config
import hydra.avro.sql.SaveMode.SaveMode
import io.confluent.kafka.schemaregistry.avro.AvroCompatibilityChecker
import org.apache.avro.generic.GenericRecord
import org.apache.avro.{Schema, SchemaNormalization}


/**
  * Created by alexsilva on 7/11/17.
  */
class AvroRecordWriter(jdbcConfig: Config, schema: Schema,
                       mode: SaveMode = SaveMode.ErrorIfExists,
                       table: Option[String] = None,
                       database: Option[String] = None) {

  private val store: Store = new StoreImpl(jdbcConfig)

  private val tableName = table.getOrElse(schema.getName)

  private val fingerprint = SchemaNormalization.parsingFingerprint64(schema)

  private val tableId = TableIdentifier(tableName, database)

  val tableObj: Table =
    store.getTable(tableId).map { table =>
      mode match {
        case SaveMode.Ignore => table
        case SaveMode.Append => table
        case SaveMode.ErrorIfExists => throw new AnalysisException(s"Table $table already exists.")
      }
    }.recover {
      case _: NoSuchTableException =>
        store.createTable(Table(tableName, schema, database))
      case e: Throwable => throw e
    }.get


  def write(record: GenericRecord) = {
    val recordFingerprint = SchemaNormalization.parsingFingerprint64(record.getSchema)
    if (recordFingerprint != fingerprint && !AvroCompatibilityChecker.BACKWARD_CHECKER
      .isCompatible(schema, record.getSchema)) {
      throw new IllegalArgumentException("Schemas are not compatible.")
    }

    store.insertRecord(record, tableObj)
  }
}