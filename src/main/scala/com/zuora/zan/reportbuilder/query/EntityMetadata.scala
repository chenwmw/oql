package com.zuora.zan.reportbuilder.query
import java.util.Date
import java.lang.reflect.Field
import com.datastax.driver.mapping.annotations.Column
import scala.collection.mutable.Map

class EntityMetadata[K](val entityClass: Class[K]) extends Metadata {
  val fields: Map[Class[_], Map[String, Field]] = Map()
  protected def getFieldName(field: Field): String = {
    val column = field.getAnnotation(classOf[Column])
    if (column == null) field.getName else column.name
  }
  
  private def getFieldsFrom(klass: Class[_]): Array[Field] = if(klass == null) Array() else  getFieldsFrom(klass.getSuperclass) ++ klass.getDeclaredFields
  
  protected def getFieldFromMap(name: String, klass: Class[_]): Field = {
    if (!fields.contains(klass))
      fields(klass) = (Map[String, Field]() /: getFieldsFrom(klass))((m, f) => m + (getFieldName(f) -> f))
    fields(klass)(name)
  }
  protected def getField(name: String, klass: Class[_]): Field = {
    if (klass.isPrimitive()) null
    else {
      val dot = name.indexOf('.')
      val name0 = if (dot == -1) name else name.substring(0, dot)
      val field = getFieldFromMap(name0, klass)
      if (dot == -1) field
      else getField(name.substring(dot + 1), field.getType)
    }
  }
  protected def getField(name: String): Field = getField(name, entityClass)
  def getType(name: String): Type = {
    val field = getField(name)
    if (field == null) null
    else {
      val fieldType = field.getType
      if (fieldType == classOf[Byte])
        BYTE
      else if (fieldType == classOf[Char])
        CHAR
      else if (fieldType == classOf[Short])
        SHORT
      else if (fieldType == classOf[Int])
        INT
      else if (fieldType == classOf[Long])
        LONG
      else if (fieldType == classOf[Float])
        FLOAT
      else if (fieldType == classOf[Double])
        DOUBLE
      else if (fieldType == classOf[Boolean])
        BOOLEAN
      else if (fieldType == classOf[String])
        STRING
      else if (fieldType == classOf[Date])
        DATE
      else if (classOf[Enum[_]].isAssignableFrom(fieldType)) {
        STRING
      } else
        null
    }
  }
}
object EntityMetadata {
  def apply[K](eClass: Class[K]): Metadata = new EntityMetadata(eClass)
}
