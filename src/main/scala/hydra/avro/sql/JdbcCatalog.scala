/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package hydra.avro.sql

import java.sql._
import java.util.Objects
import javax.sql.DataSource

import hydra.avro.util.TryWith
import org.apache.avro.{AvroRuntimeException, Schema}
import org.apache.avro.Schema.Field
import org.slf4j.LoggerFactory

import scala.util.{Failure, Success, Try}


/**
  * Internal implementation of the user-facing `Catalog`.
  */
class JdbcCatalog(ds: DataSource, dbSyntax: DbSyntax, dialect: JdbcDialect) extends Catalog with JdbcHelper {

  import JdbcCatalog.log

  override def createSchema(schema: String): Boolean = {
    withConnection(ds.getConnection) { conn =>
      validateName(schema)
      Try(JdbcUtils.createSchema(schema, "", conn)).map(_ => true)
        .recover { case e: SQLException => throw UnableToCreateException(e.getMessage) }
        .get
    }
  }

  override def createOrAlterTable(table: Table): Boolean = {
    withConnection(ds.getConnection) { conn =>
      validateName(table.name)
      val tableExists = JdbcUtils.tableExists(conn, dialect, table.name)

      Try(JdbcUtils.createTable(table.schema, dialect, table.name, "", dbSyntax, conn))
        .flatMap(_ => alterIfNeeded(table, conn))
        .recover { case e: SQLException => throw UnableToCreateException(e.getMessage) }
        .get
    }
  }

  private def alterIfNeeded(table: Table, connection: Connection): Try[Boolean] = {
    //todo: make this a config
    val autoEvolve = true
    getTableMetadata(TableIdentifier(table.name)).flatMap { tableMetadata =>
      val dbColumns = tableMetadata.columns
      findMissingFields(table.schema, dbColumns) match {
        case Nil => Success(false)
        case fields =>
          val invalidFields = fields.find(f => JdbcUtils.isNullableUnion(f.schema()) && f.defaultVal() == null)
          invalidFields.foreach { f =>
            throw new AvroRuntimeException(s"Cannot ALTER to add missing field ${f.name()}, as it is not optional and does not have a default value")
          }
          if (!autoEvolve) {
            throw new RuntimeException(s"Table ${table.name} is missing fields ${fields.map(_.name())} and auto-evolution is disabled")
          }

          val alterQueries = dialect.alterTableQueries(table.name, fields, dbSyntax)
          log.info("Amending table to add missing fields:{} maxRetries:{} with SQL: {}", fields, alterQueries)
          TryWith(connection.createStatement())(stmt => alterQueries.foreach(stmt.executeUpdate))
          Success(true)
      }
    }
  }

  def findMissingFields(schema: Schema, columns: Seq[DbColumn]): Seq[Field] = {
    import scala.collection.JavaConverters._
    val fields = schema.getFields.asScala
    val missing = fields.map(_.name()).diff(columns.map(_.name))
    missing.map(schema.getField)
  }


  override def tableExists(name: TableIdentifier): Boolean = synchronized {
    withConnection(ds.getConnection) { conn =>
      val db = name.schema.map(d => formatDatabaseName(d) + ".")
      val table = db.getOrElse("") + formatTableName(name.table)
      JdbcUtils.tableExists(conn, dialect, table)
    }
  }


  override def getTableMetadata(tableId: TableIdentifier): Try[DbTable] = {
    withConnection(ds.getConnection) { conn =>
      val table = formatTableName(tableId.table)
      tableId.schema.foreach(requireSchemaExists(_, conn))
      val db = tableId.schema.getOrElse("")
      val exists = JdbcUtils.tableExists(conn, dialect, table)
      if (exists) doGetTableMetadata(table, conn) else Failure(new NoSuchTableException(db, table))

    }
  }

  private[sql] def validateName(name: String): Unit = {
    val validNameFormat = "([\\w_]+)".r
    if (!validNameFormat.pattern.matcher(name).matches()) {
      throw AnalysisException(s"`$name` is not a valid name for tables/databases. " +
        "Valid names only contain alphabet characters, numbers and _.")
    }
  }

  private def requireSchemaExists(db: String, conn: Connection): Unit = {
    if (!checkSchemaExists(db, conn)) throw new NoSuchSchemaException(db)
  }

  def schemaExists(schema: String): Boolean = {
    withConnection(ds.getConnection)(checkSchemaExists(schema, _))
  }

  private def checkSchemaExists(schema: String, conn: Connection): Boolean = {
    val dbName = formatDatabaseName(schema)
    val rs = conn.getMetaData.getSchemas
    new Iterator[String] {
      def hasNext = rs.next()

      def next() = rs.getString(1)

    }.find(_.equalsIgnoreCase(dbName)).isDefined
  }

  protected[this] def formatTableName(name: String): String = {
    val tableName = if (dialect.caseSensitiveAnalysis) name else name.toLowerCase
    dbSyntax.format(tableName)
  }

  protected[this] def formatDatabaseName(name: String): String = {
    if (dialect.caseSensitiveAnalysis) name else name.toLowerCase
  }


  private def getSchema(connection: Connection, product: String): String = {
    product match {
      case s if s matches "(?i)oracle" =>
        TryWith(connection.createStatement()) { stmt =>
          val rs = stmt.executeQuery("select sys_context('userenv','current_schema') x from dual")
          if (rs.next()) {
            rs.getString("x").toUpperCase
          } else {
            throw new SQLException("Failed to determine Oracle schema")
          }
        }.get
      case s if s.toLowerCase.startsWith("postgres") => connection.getSchema
      case _ => null //ok to return here, because that's how jdbc works
    }
  }

  private def doGetTableMetadata(tableName: String, connection: Connection): Try[DbTable] = {
    val dbMetaData = connection.getMetaData
    val product = dbMetaData.getDatabaseProductName
    val catalog = connection.getCatalog
    val schema = getSchema(connection, product)
    val tableNameForQuery = if (product.equalsIgnoreCase("oracle")) tableName.toUpperCase else tableName
    JdbcCatalog.log.info("Querying column metadata for product:{} schema:{} catalog:{} table:{}",
      product, schema, catalog, tableNameForQuery)

    val pkColumns: Seq[String] = TryWith(dbMetaData.getPrimaryKeys(catalog, schema, tableNameForQuery)) { rs =>
      val rsIter = new Iterator[String] {
        def hasNext = rs.next()

        def next() = rs.getString("COLUMN_NAME")
      }

      rsIter.toSeq
    }.get

    val columns: Seq[DbColumn] = TryWith(dbMetaData.getColumns(catalog, schema, tableNameForQuery, null)) { rs =>
      val rsIter = new Iterator[DbColumn] {
        def hasNext = rs.next()

        def next() = {
          val colName = rs.getString("COLUMN_NAME")
          val sqlType = rs.getInt("DATA_TYPE")
          val remarks = rs.getString("REMARKS")
          val isPk = pkColumns.find(_ == colName).isDefined
          val isNullable = !isPk && Objects.equals("YES", rs.getString("IS_NULLABLE"))
          val jtype = JDBCType.valueOf(sqlType)
          DbColumn(colName, jtype, isNullable, Option(remarks))
        }
      }

      rsIter.toSeq
    }.get

    Success(DbTable(tableName, columns))
  }


}

object JdbcCatalog {
  val log = LoggerFactory.getLogger(getClass)
}