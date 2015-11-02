package org.apache.flink.api.table

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeInfoParser
import org.apache.flink.api.table.typeinfo.RowTypeInfo


case class Schema(fields: Field*) extends Serializable{

  private val nameToIndex: Map[String, Int] =
    fields.map(_.name).zipWithIndex.toMap

  if (nameToIndex.size != fields.size) {
    throw new IllegalArgumentException("Field names must be unique.")
  }


  def this(copyFrom: Schema, fields: Field*) =
    this(copyFrom.fields ++ fields: _*)


  def index(fieldName: String): Int = nameToIndex(fieldName)

  def add(field: Field): Schema =
    new Schema(this, field)

  def add(fieldName: String, typeInfo: TypeInformation[_]): Schema =
    add(Field(fieldName, typeInfo))

  def add(fieldName: String, typeInfoString: String): Schema =
    add(Field(fieldName, typeInfoString))

  def getValue[T](row: Row, fieldName: String): T =
    row.getAs[T](index(fieldName))

}

object Schema {
  implicit def typeInfo(schema: Schema): TypeInformation[Row] = {
    new RowTypeInfo(schema.fields.map(_.typeInfo), schema.fields.map(_.name))
  }
}


case class Field(name: String, typeInfo: TypeInformation[_])
        extends Serializable {
}

object Field {
  def apply(name: String, typeInfoString: String) =
    new Field(name, TypeInfoParser.parse(typeInfoString))
}
