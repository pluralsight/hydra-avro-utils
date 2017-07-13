package hydra.avro.sql

import java.sql.{Connection, JDBCType}

import org.apache.avro.LogicalTypes.Decimal
import org.apache.avro.Schema.Type
import org.apache.avro.Schema.Type._
import org.apache.avro.{LogicalTypes, Schema}
import org.slf4j.LoggerFactory

import scala.util.Try

/**
  * Created by alexsilva on 5/4/17.
  */
private[avro] object JdbcUtils {

  import scala.collection.JavaConverters._

  val nullSchema = Schema.create(Type.NULL)

  val logger = LoggerFactory.getLogger(getClass)

  def getCommonJDBCType(dt: Schema): Option[JdbcType] = {
    dt.getType match {
      case INT => commonIntTypes(dt)
      case BYTES => commonByteTypes(dt)
      case UNION => commonUnionTypes(dt)
      case _ => numberTypes(dt)
    }
  }


  private def commonIntTypes(schema: Schema): Option[JdbcType] = {
    if (isLogicalType(schema, "date")) {
      Option(JdbcType("DATE", JDBCType.DATE))
    } else {
      Option(JdbcType("INTEGER", JDBCType.INTEGER))
    }
  }

  private def commonByteTypes(schema: Schema): Option[JdbcType] = {
    if (isLogicalType(schema, "decimal")) {
      val lt = LogicalTypes.fromSchema(schema).asInstanceOf[Decimal]
      Option(JdbcType(s"DECIMAL(${lt.getPrecision},${lt.getScale})", JDBCType.DECIMAL))
    }
    else {
      Option(JdbcType("BYTE", JDBCType.TINYINT))
    }
  }

  private def commonUnionTypes(dt: Schema): Option[JdbcType] = dt.getType match {
    case UNION if (isNullableUnion(dt)) => getCommonJDBCType(getNonNullableUnionType(dt))
    case _ => throw new IllegalArgumentException(s"Only nullable unions of two elements are supported.")
  }

  private def numberTypes(dt: Schema): Option[JdbcType] = dt.getType match {
    case LONG if isLogicalType(dt, "timestamp-millis") =>
      Option(JdbcType("TIMESTAMP", JDBCType.TIMESTAMP))
    case LONG => Option(JdbcType("BIGINT", JDBCType.BIGINT))
    case DOUBLE => Option(JdbcType("DOUBLE PRECISION", JDBCType.DOUBLE))
    case FLOAT => Option(JdbcType("REAL", JDBCType.FLOAT))
    case BOOLEAN => Option(JdbcType("BIT(1)", JDBCType.BIT))
    case STRING => Option(JdbcType("TEXT", JDBCType.VARCHAR))
    case _ => None
  }


  def isLogicalType(schema: Schema, name: String) = {
    Option(schema.getLogicalType).map(_.getName == name) getOrElse (false)
  }

  def getNonNullableUnionType(schema: Schema): Schema = {
    if (schema.getTypes.get(0).getType == Type.NULL) schema.getTypes.get(1) else schema.getTypes.get(0)
  }

  def isNullableUnion(schema: Schema): Boolean =
    schema.getType == Type.UNION && schema.getTypes.size == 2 && schema.getTypes().contains(nullSchema)

  /**
    * Creates a table with a given schema.
    */
  def createTable(
                   schema: Schema,
                   dialect: JdbcDialect,
                   table: String,
                   createTableOptions: String,
                   dbSyntax: DbSyntax,
                   conn: Connection): Int = {
    val strSchema = schemaString(schema, dialect, dbSyntax)
    val sql = s"CREATE TABLE $table ($strSchema) $createTableOptions"
    logger.debug(sql)
    val statement = conn.createStatement
    try {
      statement.executeUpdate(sql)
    } finally {
      statement.close()
    }
  }

  /**
    * Returns true if the table already exists in the JDBC database.
    */
  def tableExists(conn: Connection, url: String, table: String): Boolean = {
    val dialect = JdbcDialects.get(url)
    Try {
      val statement = conn.prepareStatement(dialect.getTableExistsQuery(table))
      try {
        statement.executeQuery()
      } finally {
        statement.close()
      }
    }.isSuccess
  }

  def schemaString(schema: Schema, dialect: JdbcDialect, dbSyntax: DbSyntax = NoOpSyntax): String = {
    val sb = new StringBuilder()
    schema.getFields.asScala foreach { field =>
      val name = dialect.quoteIdentifier(dbSyntax.format(field.name))
      val typ: String = getJdbcType(field.schema(), dialect).databaseTypeDefinition
      val nullable = if (isNullableUnion(field.schema())) "" else "NOT NULL"
      sb.append(s", $name $typ $nullable")
    }
    if (sb.length < 2) "" else sb.substring(2)
  }

  def columns(schema: Schema, dialect: JdbcDialect, dbSyntax: DbSyntax = NoOpSyntax): Seq[Column] = {
    columnMap(schema, dialect, dbSyntax).values.toSeq
  }

  def columnMap(schema: Schema, dialect: JdbcDialect, dbSyntax: DbSyntax = NoOpSyntax): Map[Schema.Field, Column] = {
    schema.getFields.asScala.map { field =>
      val name = dbSyntax.format(field.name)
      val typ = getJdbcType(field.schema(), dialect)
      val nullable = isNullableUnion(field.schema())
      field -> Column(name, typ, nullable, field.schema(), Option(field.doc()))
    }.toMap
  }

  def columnNames(schema: Schema, dbSyntax: DbSyntax = NoOpSyntax): Seq[String] = {
    schema.getFields.asScala.map(f => dbSyntax.format(f.name()))
  }

  def insertStatement(table: String, schema: Schema,
                      dialect: JdbcDialect,dbs: DbSyntax): String = {
    import scala.collection.JavaConverters._
    val columns = schema.getFields.asScala
    val cols = columns.map(c => dialect.quoteIdentifier(dbs.format(c.name))).mkString(",")
    val placeholders = schema.getFields.asScala.map(_ => "?").mkString(",")
    s"INSERT INTO $table ($cols) VALUES ($placeholders)"
  }

  private def getJdbcType(schema: Schema, dialect: JdbcDialect): JdbcType = {
    dialect.getJDBCType(schema).orElse(getCommonJDBCType(schema)).getOrElse(
      throw new IllegalArgumentException(s"Can't get JDBC type for ${schema.getName}"))
  }
}
