package hydra.avro.sql

import java.sql.JDBCType

import org.apache.avro.LogicalTypes.Decimal
import org.apache.avro.Schema.Type.{BYTES, UNION}
import org.apache.avro.Schema.{Field, Type}
import org.apache.avro.{LogicalTypes, Schema}

/**
  * Created by alexsilva on 5/4/17.
  */
private[sql] object PostgresDialect extends JdbcDialect {

  override val jsonPlaceholder = "to_json(?)"

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


  override def buildUpsert(table: String, schema: Schema, dbs: DbSyntax, idFields: Seq[Field]): String = {
    import scala.collection.JavaConverters._
    val fields = schema.getFields.asScala
    val columns = fields.map(f => quoteIdentifier(dbs.format(f.name))).mkString(",")
    val placeholders = parameterize(fields)
    val updateSchema = fields -- idFields
    val updateColumns = updateSchema.map(f => quoteIdentifier(f.name)).mkString(",")
    val updatePlaceholders = parameterize(updateSchema)
    val whereClause = idFields.map(c => s"$table.${c.name()}=?").mkString(" and ")
    val sql =
      s"""insert into $table ($columns) values (${placeholders.mkString(",")})
         |on conflict (${idFields.map(_.name).mkString(",")})
         |do update set ($updateColumns) = (${updatePlaceholders.mkString(",")})
         |where $whereClause;""".stripMargin

    sql
  }

  private def bytesType(schema: Schema): Option[JdbcType] = {
    if (JdbcUtils.isLogicalType(schema, "decimal")) {
      val lt = LogicalTypes.fromSchema(schema).asInstanceOf[Decimal]
      Option(JdbcType(s"DECIMAL(${lt.getPrecision},${lt.getScale})", JDBCType.DECIMAL))
    } else {
      throw new IllegalArgumentException(s"Unsupported type in postgresql: ${schema.getType}")
    }
  }

  override def getTableExistsQuery(table: String): String = {
    s"SELECT 1 FROM $table LIMIT 1"
  }

  override def isCascadingTruncateTable(): Option[Boolean] = Some(true)

}

