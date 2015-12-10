package com.zuora.zan.reportbuilder.query

import com.datastax.driver.core.Row

class RowAdapter[K](val eClass: Class[K]) extends EntityMetadata(eClass) with Adapter[Row] {
  def get(row: Row, key: String): Any = {
    val ctype = getType(key)
    ctype match {
      case BYTE => row.getInt(key).asInstanceOf[Byte]
      case CHAR => row.getInt(key).asInstanceOf[Char]
      case SHORT => row.getInt(key).asInstanceOf[Short]
      case INT => row.getInt(key)
      case LONG => row.getLong(key)
      case FLOAT => row.getFloat(key)
      case DOUBLE => row.getDouble(key)
      case BOOLEAN => row.getBool(key)
      case STRING => row.getString(key)
      case DATE => row.getDate(key)
      case _ => null
    }
  }
}
object RowAdapter {
  def apply[K](eClass: Class[K]): RowAdapter[K] = new RowAdapter(eClass)
}