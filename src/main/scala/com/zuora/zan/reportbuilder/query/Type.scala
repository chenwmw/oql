package com.zuora.zan.reportbuilder.query

import com.zuora.zan.reportbuilder.query.impl._
import scala.math.Numeric._

trait Type {
  def getJavaType: String
  def getWrapperType: String
  def compare(left: Expression, right: Expression, comparator: String): String = "" + left + " " + comparator + " " + right
}

object NOTHING extends Type {
  def getJavaType = null
  def getWrapperType = null
}

abstract class Number(val ord: Int) extends Type

object BYTE extends Number(0) {
  def getJavaType = "byte"
  def getWrapperType = "Byte"
}

object SHORT extends Number(1) {
  def getJavaType = "short"
  def getWrapperType = "Short"
}

object CHAR extends Number(2) {
  def getJavaType = "char"
  def getWrapperType = "Character"
}

object INT extends Number(3) {
  def getJavaType = "int"
  def getWrapperType = "Integer"
}

object LONG extends Number(4) {
  def getJavaType = "long"
  def getWrapperType = "Long"
}

object FLOAT extends Number(5) {
  def getJavaType = "float"
  def getWrapperType = "Float"
}

object DOUBLE extends Number(6) {
  def getJavaType = "double"
  def getWrapperType = "Double"
}

object STRING extends Type {
  def getJavaType = "String"
  def getWrapperType = "String"
  override def compare(left: Expression, right: Expression, comparator: String) = "compareString(" + left + ", " + right + ") " + comparator + " 0"
}

object DATE extends Type {
  def getJavaType = "Date"
  def getWrapperType = "Date"
  private def defCompare(left: Expression, right: Expression, comparator: String): String = "compareDate(" + left + ", " + right + ") " + comparator + " 0"
  override def compare(left: Expression, right: Expression, comparator: String) = left.getType match {
    case l: Number => right.getType match {
      case DATE => defCompare(left, right, comparator)
      case _ => super.compare(left, right, comparator)
    }
    case DATE => right.getType match {
      case DATE => defCompare(left, right, comparator)
      case r: Number => defCompare(left, right, comparator)
      case _ => super.compare(left, right, comparator)
    }
    case _ => super.compare(left, right, comparator)
  }
}

object BOOLEAN extends Type {
  def getJavaType = "boolean"
  def getWrapperType = "Boolean"
}