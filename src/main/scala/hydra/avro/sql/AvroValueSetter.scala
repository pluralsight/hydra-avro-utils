package hydra.avro.sql


import java.math.{MathContext, RoundingMode}
import java.sql.{PreparedStatement, Timestamp}
import java.time.{LocalDate, ZoneId}

import com.google.common.collect.Lists
import hydra.avro.sql.JdbcUtils.isLogicalType
import org.apache.avro.LogicalTypes.Decimal
import org.apache.avro.Schema.Type.LONG
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.avro.{AvroRuntimeException, LogicalTypes, Schema}

import scala.collection.JavaConverters._

/**
  * Created by alexsilva on 7/12/17.
  */
//scalastyle:off
private[sql] class AvroValueSetter(schema: Schema, dialect: JdbcDialect, dbSyntax: DbSyntax) {

  val columns = JdbcUtils.columnMap(schema, dialect, dbSyntax)

  def setValues(record: GenericRecord, stmt: PreparedStatement) = {
    schema.getFields.asScala.zipWithIndex.foreach {
      case (f, idx) =>
        setFieldValue(record.get(f.name()), columns(f), f.schema(), stmt, idx + 1)
    }
  }

  private def setFieldValue(value: AnyRef, col: Column, schema: Schema, pstmt: PreparedStatement, idx: Int): Unit = {

    if (value == null) {
      pstmt.setNull(idx, col.dataType.targetSqlType.getVendorTypeNumber.intValue())
    } else {
      schema.getType match {
        case Schema.Type.UNION => unionValue(value, col, schema, pstmt, idx)
        case Schema.Type.ARRAY => arrayValue(value.asInstanceOf[GenericData.Array[AnyRef]], col, schema, pstmt, idx)
        case Schema.Type.STRING =>
          pstmt.setString(idx, if (value == "null" || value.toString == "null") null else value.toString)
        case Schema.Type.BOOLEAN =>
          pstmt.setBoolean(idx, value.asInstanceOf[Boolean])
        case Schema.Type.DOUBLE =>
          pstmt.setDouble(idx, value.toString.toDouble: java.lang.Double)
        case Schema.Type.FLOAT => pstmt.setFloat(idx, value.toString.toFloat)
        case Schema.Type.INT if isLogicalType(schema, "date") =>
          val ld = LocalDate.ofEpochDay(value.toString.toInt)
          //todo: time zones?
          val inst = ld.atStartOfDay(ZoneId.systemDefault()).toInstant()
          pstmt.setDate(idx, new java.sql.Date(inst.toEpochMilli))
        case Schema.Type.INT => pstmt.setInt(idx, value.toString.toInt)
        case LONG if isLogicalType(schema, "timestamp-millis") =>
          pstmt.setTimestamp(idx, new Timestamp(value.toString.toLong))
        case Schema.Type.LONG => pstmt.setLong(idx, value.toString.toLong)
        case Schema.Type.BYTES => byteValue(value, schema, pstmt, idx)
        case Schema.Type.NULL =>
          pstmt.setNull(idx, col.dataType.targetSqlType.getVendorTypeNumber.intValue())
        case _ => throw new IllegalArgumentException(s"Type ${schema.getType} is not supported.")
      }
    }
  }

  private def byteValue(obj: AnyRef, schema: Schema, pstmt: PreparedStatement, idx: Int) = {
    if (isLogicalType(schema, "decimal")) {
      val lt = LogicalTypes.fromSchema(schema).asInstanceOf[Decimal]
      val ctx = new MathContext(lt.getPrecision, RoundingMode.HALF_EVEN)
      val decimal = new java.math.BigDecimal(obj.toString, ctx).setScale(lt.getScale)
      pstmt.setBigDecimal(idx, decimal)
    }
    else {
      pstmt.setBytes(idx, obj.toString.getBytes)
    }
  }

  private def arrayValue(obj: GenericData.Array[AnyRef], col: Column, schema: Schema,
                         pstmt: PreparedStatement, idx: Int): Unit = {

    val aType = JdbcUtils.getJdbcType(schema.getElementType, dialect)
    val values = Lists.newArrayList(obj.iterator()).toArray
    val arr = pstmt.getConnection.createArrayOf(aType.targetSqlType.getName,values )
    pstmt.setArray(idx, arr)
  }

  private def unionValue(obj: AnyRef, col: Column, schema: Schema, pstmt: PreparedStatement, idx: Int): Unit = {
    val types = schema.getTypes

    if (!JdbcUtils.isNullableUnion(schema)) {
      throw new AvroRuntimeException("Unions may only consist of a concrete type and null in hydra avro.")
    }
    if (types.size == 1) {
      setFieldValue(obj, col, types.get(0), pstmt, idx)
    }
    else setFieldValue(obj, col, JdbcUtils.getNonNullableUnionType(schema), pstmt, idx)

  }
}
