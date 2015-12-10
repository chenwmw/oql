package com.zuora.zan.reportbuilder.query

import java.lang.Enum
import java.lang.reflect.Field
class BeanAdapter[K](val eClass: Class[K]) extends EntityMetadata(eClass) with Adapter[K] {
  private def getValue(obj: Any, field: Field): Any = {
    field.setAccessible(true)
    val value = field.get(obj)
    if (classOf[Enum[_]].isAssignableFrom(field.getType)) {
      if (value == null) null else value.toString
    } else value
  }
  private def getValue(obj: Any, name: String, klass: Class[_]): Any = {
    if (obj == null) null
    else if (klass.isPrimitive()) null
    else {
      val dot = name.indexOf('.')
      val name0 = if (dot == -1) name else name.substring(0, dot)
      val field = getFieldFromMap(name0, klass)
      if (field == null) null
      else if (dot == -1) {
        getValue(obj, field)
      } else {
        val name1 = name.substring(dot + 1)
        getValue(getValue(obj, field), name1, field.getType)
      }
    }
  }
  def get(obj: K, key: String): Any = getValue(obj, key, entityClass)
}
object BeanAdapter {
  def apply[K](eClass: Class[K]): BeanAdapter[K] = new BeanAdapter(eClass)
}
