package hydra.avro.sql

import java.sql.JDBCType

import hydra.avro.util.AvroUtils
import org.apache.avro.LogicalTypes.Decimal
import org.apache.avro.Schema.Type.{BYTES, UNION}
import org.apache.avro.Schema.{Field, Type}
import org.apache.avro.{LogicalTypes, Schema}

/**
  * Created by alexsilva on 5/4/17.
  */
private[sql] object PostgresDialect extends JdbcDialect {

  override val jsonPlaceholder = "to_json(?::TEXT)"

  override def canHandle(url: String): Boolean = url.startsWith("jdbc:postgresql")

  override def getJDBCType(schema: Schema): Option[JdbcType] = schema.getType match {
    case Type.STRING => Some(JdbcType("TEXT", JDBCType.CHAR))
    case BYTES => bytesType(schema)
    case Type.BOOLEAN => Some(JdbcType("BOOLEAN", JDBCType.BOOLEAN))
    case Type.FLOAT => Some(JdbcType("FLOAT4", JDBCType.FLOAT))
    case Type.DOUBLE => Some(JdbcType("FLOAT8", JDBCType.DOUBLE))
    case UNION => unionType(schema)
    case Type.RECORD => Some(JdbcType("JSON", JDBCType.CHAR))
    case Type.ARRAY => getArrayType(schema)
    case _ => None
  }

  private def getArrayType(schema: Schema) = {
    getJDBCType(schema.getElementType).map(_.databaseTypeDefinition)
      .orElse(JdbcUtils.getCommonJDBCType(schema.getElementType).map(_.databaseTypeDefinition))
      .map { typeName =>
        //we don't support json arrays yet.
        val arrayType = if (typeName == "JSON") "TEXT" else typeName
        JdbcType(s"$arrayType[]", java.sql.JDBCType.ARRAY)
      }
  }

  private def unionType(schema: Schema): Option[JdbcType] = {
    if (JdbcUtils.isNullableUnion(schema)) {
      getJDBCType(JdbcUtils.getNonNullableUnionType(schema))
    } else {
      throw new IllegalArgumentException(s"Only nullable unions of two elements are supported.")
    }
  }

  override def upsertFields(schema: Schema): Seq[Field] = {
    import scala.collection.JavaConverters._
    val fields = schema.getFields.asScala
    val idFields = AvroUtils.getPrimaryKeys(schema)
    val updateSchema = if (idFields.isEmpty) Seq.empty else fields -- idFields
    fields ++ updateSchema ++ idFields
  }

  override def buildUpsert(table: String, schema: Schema, dbs: DbSyntax): String = {
    import scala.collection.JavaConverters._

    def formatColName(col: Field) = quoteIdentifier(dbs.format(col.name()))

    val idFields = AvroUtils.getPrimaryKeys(schema)
    val fields = schema.getFields.asScala
    val columns = fields.map(formatColName).mkString(",")
    val placeholders = parameterize(fields)
    val updateSchema = fields -- idFields
    val updateColumns = updateSchema.map(formatColName).mkString(",")
    val updatePlaceholders = parameterize(updateSchema)
    val whereClause = idFields.map(c => s"$table.${formatColName(c)}=?").mkString(" and ")

    val sql =
      s"""insert into $table ($columns) values (${placeholders.mkString(",")})
         |on conflict (${idFields.map(formatColName).mkString(",")})
         |do update set ($updateColumns) = (${updatePlaceholders.mkString(",")})
         |where $whereClause;""".stripMargin

    sql

  }

  private def bytesType(schema: Schema): Option[JdbcType] = {
    if (JdbcUtils.isLogicalType(schema, "decimal")) {
      val lt = LogicalTypes.fromSchema(schema).asInstanceOf[Decimal]
      Option(JdbcType(s"DECIMAL(${lt.getPrecision},${lt.getScale})", JDBCType.DECIMAL))
    } else {
      Option(JdbcType(s"BYTEA", JDBCType.BINARY))
    }
  }

  override def getTableExistsQuery(table: String): String = {
    s"SELECT 1 FROM $table LIMIT 1"
  }

  override def isCascadingTruncateTable(): Option[Boolean] = Some(true)

  override def alterTableQueries(table: String, missingFields: Seq[Field], dbs: DbSyntax): Seq[String] = {
    missingFields.map { f =>
      val dbDef = JdbcUtils.getJdbcType(f.schema, this).databaseTypeDefinition
      val colName = quoteIdentifier(dbs.format(f.name))
      s"alter table $table add column $colName $dbDef"
    }
  }

  override def tableNameForMetadataQuery(tableName: String): String = tableName.toLowerCase

}

