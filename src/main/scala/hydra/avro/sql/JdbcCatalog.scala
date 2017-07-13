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

import hydra.avro.util.JdbcHelper
import org.apache.avro.Schema

import scala.util.{Failure, Success, Try}


/**
  * Internal implementation of the user-facing `Catalog`.
  */
class JdbcCatalog(ds: DataSource, dbSyntax: DbSyntax, dialect: JdbcDialect) extends Catalog with JdbcHelper {


  override def createTable(table: Table): Table = {
    withConnection(ds.getConnection) { conn =>
      val name = table.database.map(_ + ".").getOrElse("") + dbSyntax.format(table.name)
      if (JdbcUtils.createTable(table.schema, dialect, name, "", dbSyntax, conn) != 0) {
        throw new SQLException("Unable to save table.")
      }
    }

    table
  }

  override def tableExists(name: TableIdentifier): Boolean = synchronized {
    withConnection(ds.getConnection) { conn =>
      val db = formatDatabaseName(name.database.getOrElse(""))
      val table = formatTableName(name.table)
      JdbcUtils.tableExists(conn, db, table)
    }
  }


  override def getTable(tableId: TableIdentifier): Try[Table] = {
    withConnection(ds.getConnection) { conn =>
      val table = formatTableName(tableId.table)
      tableId.database.foreach(requireDbExists(_, conn))
      val db = tableId.database.getOrElse("")
      Try(if (JdbcUtils.tableExists(conn, db, table)) Success(true) else Failure(new NoSuchTableException(db, table)))
        .flatMap(_ => doGetTable(tableId.database, table, conn))
    }
  }

  private def validateName(name: String): Unit = {
    val validNameFormat = "([\\w_]+)".r
    if (!validNameFormat.pattern.matcher(name).matches()) {
      throw AnalysisException(s"`$name` is not a valid name for tables/databases. " +
        "Valid names only contain alphabet characters, numbers and _.")
    }
  }

  private def requireDbExists(db: String, conn: Connection): Unit = {
    if (!databaseExists(db, conn)) {
      throw new NoSuchDatabaseException(db)
    }
  }

  private def databaseExists(db: String, conn: Connection): Boolean = {
    val dbName = formatDatabaseName(db)

    val rs = conn.getMetaData.getCatalogs
    new Iterator[String] {
      def hasNext = rs.next()

      def next() = rs.getString(1)
    }.find(_ == dbName).isDefined

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
  private def doGetTable(dbName: Option[String], tableName: String, conn: Connection): Try[Table] = {
    val md = conn.getMetaData
    val rs = md.getTables(null, dbName.getOrElse(null), "%", null)


    new Iterator[String] {
      def hasNext = rs.next()

      def next() = rs.getString(3)

      //TODO: generate schema from jdbc
    }.find(_ == tableName).map(_ => Success(Table(tableName, Schema.create(Schema.Type.NULL), dbName)))
      .getOrElse(throw new NoSuchTableException(db = dbName.getOrElse(""), table = tableName))
  }
}

