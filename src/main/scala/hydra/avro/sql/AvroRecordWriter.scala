package hydra.avro.sql

import java.sql.PreparedStatement
import java.util.concurrent.TimeUnit

import com.google.common.cache.{Cache, CacheBuilder, RemovalListener, RemovalNotification}
import com.typesafe.config.Config
import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import hydra.avro.sql.SaveMode.SaveMode
import hydra.avro.util.ConfigUtils._
import io.confluent.kafka.schemaregistry.avro.AvroCompatibilityChecker
import org.apache.avro.generic.GenericRecord
import org.apache.avro.{AvroRuntimeException, Schema, SchemaNormalization}
import org.slf4j.LoggerFactory
import scala.collection.JavaConverters._


/**
  * Created by alexsilva on 7/11/17.
  *
  * A batch size of 0 means that this class will never do any executeBatch and that external clients need to call
  * flush()
  */
class AvroRecordWriter(jdbcConfig: Config, schema: Schema,
                       mode: SaveMode = SaveMode.ErrorIfExists,
                       checkSchemaCompatibility: Boolean,
                       dbSyntax: DbSyntax = UnderscoreSyntax,
                       batchSize: Int = 50,
                       table: Option[String] = None,
                       database: Option[String] = None) {

  import AvroRecordWriter._

  private val ds = new HikariDataSource(new HikariConfig(jdbcConfig))

  private val dialect = JdbcDialects.get(jdbcConfig.getString("dataSource.url"))

  private val store: Catalog = new JdbcCatalog(ds, dbSyntax, dialect)

  private val tableName = table.getOrElse(schema.getName)

  private val tableId = TableIdentifier(tableName, database)

  private lazy val conn = ds.getConnection

  private val valueSetter = new AvroValueSetter(schema, dialect, dbSyntax)

  private val cache: Cache[Long, PreparedStatement] = CacheBuilder.newBuilder()
    .expireAfterWrite(10, TimeUnit.MINUTES).removalListener(removalListener)
    .maximumSize(1000).build().asInstanceOf[Cache[Long, PreparedStatement]]

  private var rowCount = 0L

  private val tableObj: Table =
    store.getTable(tableId).map { table =>
      mode match {
        case SaveMode.Ignore => table
        case SaveMode.Append => table
        case SaveMode.ErrorIfExists => throw new AnalysisException(s"Table $table already exists.")
      }
    }.recover {
      case _: NoSuchTableException =>
        val table = Table(tableName, schema, database)
        store.createTable(table)
        table
      case e: Throwable => throw e
    }.get


  def write(record: GenericRecord): Unit = {
    if (checkSchemaCompatibility && !AvroCompatibilityChecker.BACKWARD_CHECKER.isCompatible(schema, record.getSchema)) {
      throw new AvroRuntimeException("Schemas are not compatible.")
    }

    val fingerprint = SchemaNormalization.parsingFingerprint64(record.getSchema)
    val stmt = cache.get(fingerprint, () => {
      val name = tableObj.dbSchema.map(_ + ".").getOrElse("") + dbSyntax.format(tableObj.name)
      val insertStmt = JdbcUtils.insertStatement(dbSyntax.format(name), record.getSchema, dialect, dbSyntax)
      logger.debug(s"Creating new prepared statement $insertStmt")
      conn.prepareStatement(insertStmt)
    })

    valueSetter.setValues(record, stmt)
    stmt.addBatch()
    rowCount += 1
    if (batchSize > 0 && rowCount % batchSize == 0) {
      stmt.executeBatch()
      rowCount = 0
    }
  }

  def flush(): Unit = {
    cache.asMap().values().asScala.foreach {
      stmt => stmt.executeBatch()
    }
  }

  def close(): Unit = {
    cache.asMap().values().asScala.foreach { stmt => stmt.executeBatch(); stmt.close() }
    conn.close()
    ds.close()
  }
}

object AvroRecordWriter {
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