package hydra.avro.sql

import java.sql.{BatchUpdateException, PreparedStatement}
import java.util.concurrent.TimeUnit

import com.google.common.cache.{Cache, CacheBuilder, RemovalListener, RemovalNotification}
import com.typesafe.config.Config
import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import hydra.avro.io.{RecordWriter, SaveMode}
import hydra.avro.io.SaveMode.SaveMode
import hydra.avro.util.ConfigUtils._
import io.confluent.kafka.schemaregistry.avro.AvroCompatibilityChecker
import org.apache.avro.generic.GenericRecord
import org.apache.avro.{AvroRuntimeException, Schema, SchemaNormalization}
import org.slf4j.LoggerFactory
import AvroCompatibilityChecker.NO_OP_CHECKER

import scala.collection.JavaConverters._
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
class JdbcRecordWriter(jdbcConfig: Config,
                       val schema: Schema,
                       val mode: SaveMode = SaveMode.ErrorIfExists,
                       override val compatibilityChecker: AvroCompatibilityChecker = NO_OP_CHECKER,
                       dbSyntax: DbSyntax = UnderscoreSyntax,
                       batchSize: Int = 50,
                       table: Option[String] = None,
                       database: Option[String] = None) extends RecordWriter {

  import JdbcRecordWriter._

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
      case Failure(ex) => ds.close(); throw ex;
    }
  }


  def write(record: GenericRecord): Unit = {
    if (!compatibilityChecker.isCompatible(schema, record.getSchema)) {
      throw new AvroRuntimeException("Schemas are not compatible.")
    }

    val fingerprint = SchemaNormalization.parsingFingerprint64(record.getSchema)
    val stmt = cache.get(fingerprint, () => {
      val pk = JdbcUtils.getIdFields(record.getSchema)
      val name = tableObj.dbSchema.map(_ + ".").getOrElse("") + dbSyntax.format(tableObj.name)
      val insertStmt = dialect.insertStatement(dbSyntax.format(name), schema, dbSyntax)
      logger.debug(s"Creating new prepared statement $insertStmt")
      conn.prepareStatement(insertStmt)
    })

    valueSetter.setValues(record, stmt)
    stmt.addBatch()
    rowCount += 1
    if (batchSize > 0 && rowCount % batchSize == 0) {
      executeBatch(stmt)
      rowCount = 0
    }
  }


  def flush(): Unit = {
    cache.asMap().values().asScala.foreach(executeBatch)
  }

  def close(): Unit = {
    cache.asMap().values().asScala.foreach {
      stmt =>
        executeBatch(stmt)
        stmt.close()
    }
    conn.close()
    ds.close()
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