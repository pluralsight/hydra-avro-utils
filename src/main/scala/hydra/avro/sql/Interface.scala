package hydra.avro.sql

import java.sql.JDBCType

import org.apache.avro.Schema

/**
  * Created by alexsilva on 7/11/17.
  */
case class Database(name: String, locationUri: String, description: Option[String])

case class Table(name: String, schema: Schema, description: Option[String] = None)

case class Column(name: String, schema: Schema, dataType: JdbcType, nullable: Boolean, description: Option[String])

case class DbTable(name: String, columns: Seq[DbColumn], description: Option[String] = None)

case class DbColumn(name: String, jdbcType: JDBCType, nullable: Boolean, description: Option[String])


