package com.zuora.zan.reportbuilder.query
import java.util.Date
import java.lang.reflect.Field
import com.datastax.driver.mapping.annotations.Column
import scala.collection.mutable.Map

class ParamMetadata extends Metadata {
  val params: java.util.Map[String, Class[_]] = new java.util.HashMap[String, Class[_]]()
  def add(name: String, pType: Class[_]) = { params.put(name, pType) }
  def getType(name: String): Type = {
    val fieldType = params.get(name)
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
