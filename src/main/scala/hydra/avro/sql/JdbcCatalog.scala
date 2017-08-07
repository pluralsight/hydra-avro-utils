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

import java.sql.{Connection, SQLException}
import javax.sql.DataSource

import org.apache.avro.Schema

import scala.util.{Failure, Success, Try}


/**
  * Internal implementation of the user-facing `Catalog`.
  */
class JdbcCatalog(ds: DataSource, dbSyntax: DbSyntax, dialect: JdbcDialect) extends Catalog with JdbcHelper {

  override def createSchema(schema: String): Boolean = {
    withConnection(ds.getConnection) { conn =>
      validateName(schema)
      Try(JdbcUtils.createSchema(schema, "", conn)).map(_ => true)
        .recover { case e: SQLException => throw UnableToCreateException(e.getMessage) }
        .get
    }
  }

  override def createTable(table: Table): Boolean = {
    withConnection(ds.getConnection) { conn =>
      validateName(table.name)
      val name = table.dbSchema.map(_ + ".").getOrElse("") + dbSyntax.format(table.name)
      Try(JdbcUtils.createTable(table.avroSchema, dialect, name, "", dbSyntax, conn)).map(_ => true)
        .recover { case e: SQLException => throw UnableToCreateException(e.getMessage) }
        .get
    }
  }


  override def tableExists(name: TableIdentifier): Boolean = synchronized {
    withConnection(ds.getConnection) { conn =>
      val db = name.schema.map(d => formatDatabaseName(d) + ".")
      val table = db.getOrElse("") + formatTableName(name.table)
      JdbcUtils.tableExists(conn, dialect, table)
    }
  }


  override def getTable(tableId: TableIdentifier): Try[Table] = {
    withConnection(ds.getConnection) { conn =>
      val table = formatTableName(tableId.table)
      tableId.schema.foreach(requireSchemaExists(_, conn))
      val db = tableId.schema.getOrElse("")
      val exists = JdbcUtils.tableExists(conn, dialect, table)
      if (exists) doGetTable(tableId.schema, table, conn) else Failure(new NoSuchTableException(db, table))

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


  /**
    * Get the table or view with the specified name in the specified database. This throws an
    * `AnalysisException` when no `Table` can be found.
    */
  private def doGetTable(schema: Option[String], tableName: String, conn: Connection): Try[Table] = {
    val md = conn.getMetaData
    val rs = md.getTables(null, schema.map(_.toUpperCase).orNull, "%", null)
    new Iterator[String] {
      def hasNext = rs.next()

      def next() = rs.getString(3)

      //TODO: generate schema from jdbc
    }.find(_.equalsIgnoreCase(tableName)).map(_ => Success(Table(tableName, Schema.create(Schema.Type.NULL), schema)))
      .getOrElse(throw new NoSuchTableException(schema = schema.getOrElse(""), table = tableName))
  }

}

