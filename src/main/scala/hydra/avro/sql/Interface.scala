package hydra.avro.sql

import org.apache.avro.Schema

/**
  * Created by alexsilva on 7/11/17.
  */
case class Database(name: String, locationUri: String, description: Option[String])

case class Table(name: String, avroSchema: Schema, dbSchema: Option[String] = None, description: Option[String] = None)

case class Column(name: String, dataType: JdbcType, nullable: Boolean, schema: Schema, description: Option[String])

