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

import java.sql.SQLException
import java.util.Properties
import java.util.concurrent.TimeUnit

import com.google.common.cache.{Cache, CacheBuilder}
import com.typesafe.config.Config
import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import org.apache.avro.generic.GenericRecord
import org.apache.avro.{Schema, SchemaNormalization}

import scala.collection.JavaConverters._
import scala.util.{Success, Try}


/**
  * Internal implementation of the user-facing `Catalog`.
  */
class StoreImpl(config: Config) extends Store {

  private val properties = new Properties

  config.entrySet().asScala.foreach(e => properties.setProperty(e.getKey(), config.getString(e.getKey())))

  protected[sql] val hikariConfig = new HikariConfig(properties)

  private val ds = new HikariDataSource(hikariConfig)

  private val dbSyntax = UnderscoreSyntax

  private val dialect = JdbcDialects.get(config.getString("dataSource.url"))

  private val cache: Cache[Long, String] = CacheBuilder.newBuilder()
    .expireAfterWrite(10, TimeUnit.MINUTES)
    .maximumSize(1000).build().asInstanceOf[Cache[Long, String]]

  override def createTable(table: Table): Table = {
    val name = table.database.map(_ + ".").getOrElse("") + dbSyntax.format(table.name)
    if (JdbcUtils.createTable(table.schema, dialect, name, "", dbSyntax, ds.getConnection) != 0) {
      throw new SQLException("Unable to save table.")
    }
    table
  }

  def tableExists(name: TableIdentifier): Boolean = synchronized {
    val db = formatDatabaseName(name.database.getOrElse(""))
    val table = formatTableName(name.table)
    JdbcUtils.tableExists(ds.getConnection, db, table)
  }


  def getTable(tableId: TableIdentifier): Try[Table] = {
    val table = formatTableName(tableId.table)
    tableId.database.foreach(requireDbExists)
    Try(requireTableExists(TableIdentifier(table, tableId.database)))
      .flatMap(_ => doGetTable(tableId.database, table))
  }

  private def validateName(name: String): Unit = {
    val validNameFormat = "([\\w_]+)".r
    if (!validNameFormat.pattern.matcher(name).matches()) {
      throw AnalysisException(s"`$name` is not a valid name for tables/databases. " +
        "Valid names only contain alphabet characters, numbers and _.")
    }
  }

  private def requireDbExists(db: String): Unit = {
    if (!databaseExists(db)) {
      throw new NoSuchDatabaseException(db)
    }
  }

  def databaseExists(db: String): Boolean = {
    val dbName = formatDatabaseName(db)
    val rs = ds.getConnection.getMetaData.getCatalogs
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

  private def requireTableExists(name: TableIdentifier): Unit = {
    if (!tableExists(name)) {
      val db = name.database.getOrElse("")
      throw new NoSuchTableException(db = db, table = name.table)
    }
  }

  /**
    * Get the table or view with the specified name in the specified database. This throws an
    * `AnalysisException` when no `Table` can be found.
    */
  private def doGetTable(dbName: Option[String], tableName: String): Try[Table] = {
    val md = ds.getConnection().getMetaData
    val rs = md.getTables(null, dbName.getOrElse(null), "%", null)

    new Iterator[String] {
      def hasNext = rs.next()

      def next() = rs.getString(1)

      //TODO: generate schema from jdbc
    }.find(_ == tableName).map(_ => Success(Table(tableName, Schema.create(Schema.Type.NULL), dbName)))
      .getOrElse(throw new NoSuchTableException(db = dbName.getOrElse(""), table = tableName))
  }

  override def insertRecord(record: GenericRecord, table: Table): Unit = {
    val fingerprint = SchemaNormalization.parsingFingerprint64(record.getSchema)
    val stmtStr = cache.get(fingerprint, () => {
      val name = table.database.map(_ + ".").getOrElse("") + dbSyntax.format(table.name)
      JdbcUtils.insertStatement(dbSyntax.format(name), record.getSchema, dialect, dbSyntax)
    })

    val stmt = ds.getConnection.prepareStatement(stmtStr)
    new AvroPreparedStatementHelper(record, dialect, dbSyntax).setValues(stmt)

    //TODO: support batch mode
    try {
      stmt.executeUpdate()
    }
    finally {
      stmt.close()
    }
  }

  override def close(): Unit = {
    ds.close()
  }
}

