package hydra.avro.sql

import java.sql.PreparedStatement

import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.avro.{AvroRuntimeException, Schema}

import scala.collection.JavaConverters._

/**
  * Created by alexsilva on 7/12/17.
  */
//scalastyle:off
class AvroPreparedStatementHelper(record: GenericRecord, dialect: JdbcDialect, dbSyntax: DbSyntax) {

  val schema = record.getSchema

  val columns = JdbcUtils.columnMap(schema, dialect, dbSyntax)

  def setValues(stmt: PreparedStatement) = {
    schema.getFields.asScala.zipWithIndex.foreach {
      case (f, idx) =>
        setFieldValue(record.get(f.name()), f.schema(), stmt, idx + 1)
    }
  }

  private def setFieldValue(value: AnyRef, schema: Schema, pstmt: PreparedStatement, idx: Int): Unit = {
    schema.getType match {
      case Schema.Type.UNION => unionValue(value, schema, pstmt, idx)
      case Schema.Type.ARRAY => arrayValue(value.asInstanceOf[GenericData.Array[AnyRef]], schema, pstmt, idx)
      case Schema.Type.STRING =>
        pstmt.setString(idx, if (value == "null" || value.toString == "null") null else value.toString)
      case Schema.Type.NULL =>
        pstmt.setNull(idx, columns(schema.getName).dataType.targetSqlType.getVendorTypeNumber.intValue())
      case Schema.Type.BOOLEAN =>
        pstmt.setBoolean(idx, value.asInstanceOf[Boolean])
      case Schema.Type.DOUBLE =>
        pstmt.setDouble(idx, value.toString.toDouble: java.lang.Double)
      case Schema.Type.FLOAT => pstmt.setFloat(idx, value.toString.toFloat)
      case Schema.Type.INT => pstmt.setInt(idx, value.toString.toInt)
      case Schema.Type.LONG => pstmt.setLong(idx, value.toString.toLong)
      case _ => throw new IllegalArgumentException(s"Type ${schema.getType} is not supported.")
    }
  }

  private def arrayValue(obj: GenericData.Array[AnyRef], schema: Schema,
                         pstmt: PreparedStatement, idx: Int): Unit = {
    obj.iterator().asScala.map(o => setFieldValue(o, schema.getElementType, pstmt, idx)).toArray
  }

  private def unionValue(obj: AnyRef, schema: Schema, pstmt: PreparedStatement, idx: Int): Unit = {
    val types = schema.getTypes


    if (!JdbcUtils.isNullableUnion(schema)) {
      throw new AvroRuntimeException("Unions may only consist of a concrete type and null in hydra avro.")
    }
    if (types.size == 1) {
      setFieldValue(obj, types.get(0), pstmt, idx)
    }
    else setFieldValue(obj, JdbcUtils.getNonNullableUnionType(schema), pstmt, idx)

  }
}
