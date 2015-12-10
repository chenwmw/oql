package com.zuora.zan.reportbuilder.query.impl

import com.zuora.zan.reportbuilder.query._
import java.util.regex.Pattern

case class SQLExpression(val fieldList:List[String], val tableName:String, val filterExp:Expression)

trait Expression {
  def toString: String
  def getType: Type
  def validate: String
}
case class Not(val item: Expression) extends Expression {
  override def toString = "!(" + item + ")"
  def getType = BOOLEAN
  def validate: String = {
    val msg = item.validate
    if (msg != null) msg
    else {
      item.getType match {
        case BOOLEAN => null
        case _ => "Not expression requires boolean operand:" + item
      }
    }
  }
}
class IsNull(val v: Expression) extends Expression {
  override def toString = "" + v + " == null"
  def getType = BOOLEAN
  def validate: String = {
    val msg = v.validate
    if (msg != null) msg
    else {
      v.getType match {
        case STRING => null
        case DATE => null
        case _ => "Is expression requires STRING or DATE expression:" + v
      }
    }
  }
}
case class NotIsNull(val nv: Expression) extends IsNull(nv) {
  override def toString = "" + nv + " != null"
}
abstract class Logic(val left: Expression, val right: Expression, val operator: String) extends Expression {
  override def toString = "" + left + " " + operator + " " + right
  def getType = BOOLEAN
  def validate: String = {
    val lmsg = left.validate
    if (lmsg != null) lmsg
    else {
      val rmsg = right.validate
      if (rmsg != null) rmsg
      else {
        if (left.getType == BOOLEAN && right.getType == BOOLEAN) null
        else "The left and right operands of or expression should be both boolean expression"
      }
    }
  }
}
case class Or(val l: Expression, val r: Expression) extends Logic(l, r, "||")
case class And(val l: Expression, val r: Expression) extends Logic(l, r, "&&")
class Like(left: Expression, right: Expression) extends Expression {
  override def toString = "like(" + left + ", " + right + ")"
  def getType = BOOLEAN
  def validate: String = {
    val lmsg = left.validate
    if (lmsg != null) lmsg
    else {
      val rmsg = right.validate
      if (rmsg != null) rmsg
      else {
        if (left.getType == STRING && right.getType == STRING) null
        else "The left and right operands of like expression should be both string expression"
      }
    }
  }
}
case class NotLike(l: Expression, r: Expression) extends Like(l, r) {
  override def toString = "!" + super.toString
}
class In(left: Expression, right: List[Expression]) extends Expression {
  override def toString = "Arrays.asList(" + right.mkString(", ") + ").contains(" + left + ")"
  def getType = BOOLEAN
  def validate: String = {
    val lmsg = left.validate
    if (lmsg != null) lmsg
    else {
      val msgs = for {
        operand <- right
        msg = operand.validate
        if (msg != null)
      } yield msg
      if (!msgs.isEmpty) msgs mkString ", "
      else {
        val ltype = left.getType
        val operands = for {
          operand <- right
          if ((operand.getType, ltype) match {
            case (rn: Number, ln: Number) => false
            case (_, _) => true
          })
        } yield operand
        if (operands.isEmpty) null
        else "The types of" + operands.mkString(", ") + " are not compatiable with the left expression:" + left
      }
    }
  }
}
case class NotIn(l: Expression, r: List[Expression]) extends In(l, r) {
  override def toString = "!" + super.toString
}
abstract class ArithOp(val left: Expression, val right: Expression, val op: String) extends Expression {
  override def toString = "" + left + " " + op + " " + right
  def getType = left.getType match {
    case l: Number => right.getType match {
      case r: Number => if (l.ord < r.ord) r else l
      case _ => NOTHING
    }
    case _ => NOTHING
  }
  def validate: String = {
    val lmsg = left.validate
    if (lmsg != null) lmsg
    else {
      val rmsg = right.validate
      if (rmsg != null) rmsg
      else {
        left.getType match {
          case l: Number => right.getType match {
            case r: Number => null
            case _ => "Right expression should be of number type."
          }
          case _ => "Left expression should be of number type."
        }
      }
    }
  }
}
abstract class RelOp(val left: Expression, val right: Expression, val comparator: String) extends Expression {
  override def toString = getCommonType.compare(left, right, comparator)
  private def getCommonType: Type = left.getType match {
    case l: Number => right.getType match {
      case r: Number => if (l.ord < r.ord) r else l
      case DATE => DATE
      case _ => NOTHING
    }
    case DATE => right.getType match {
      case r: Number => DATE
      case DATE => DATE
      case _ => NOTHING
    }
    case STRING => right.getType match {
      case STRING => STRING
      case _ => NOTHING
    }
    case _ => NOTHING
  }
  def getType = BOOLEAN
  def validate: String = {
    val lmsg = left.validate
    if (lmsg != null) lmsg
    else {
      val rmsg = right.validate
      if (rmsg != null) rmsg
      else {
        left.getType match {
          case l: Number => right.getType match {
            case r: Number => null
            case DATE => null
            case _ => "Right expression should be of number type."
          }
          case DATE => right.getType match {
            case r: Number => null
            case DATE => null
            case _ => "Right expression should be of date or number type."
          }
          case STRING => right.getType match {
            case STRING => null
            case _ => "Right expression should be of string type."
          }
          case _ => "Left expression should be of number type."
        }
      }
    }
  }
}
case class EqualTo(val l: Expression, val r: Expression) extends RelOp(l, r, "==")
case class NotEqualTo(val l: Expression, val r: Expression) extends RelOp(l, r, "!=")
case class LessThan(val l: Expression, val r: Expression) extends RelOp(l, r, "<")
case class LessThanOrEqual(val l: Expression, val r: Expression) extends RelOp(l, r, "<=")
case class GreaterThan(val l: Expression, val r: Expression) extends RelOp(l, r, " > ")
case class GreaterThanOrEqual(val l: Expression, val r: Expression) extends RelOp(l, r, ">=")
case class Add(val l: Expression, val r: Expression) extends ArithOp(l, r, "+")
case class Subtract(val l: Expression, val r: Expression) extends ArithOp(l, r, "-")
case class Multiply(val l: Expression, val r: Expression) extends ArithOp(l, r, "*")
case class Divide(val l: Expression, val r: Expression) extends ArithOp(l, r, "/")
case class Remainder(val l: Expression, val r: Expression) extends ArithOp(l, r, "%")
case class UnaryMinus(val item: Expression) extends Expression {
  override def toString = "-" + item
  def getType = item.getType
  def validate: String = item.validate
}
case class IdentValue(val keys: List[String], val itype: Type) extends Expression {
  override def toString = keys mkString "_"
  def getType = itype
  def validate: String = null
}
case class ParamValue(val key: String, val itype: Type) extends Expression {
  override def toString = "this." + key
  def getType = itype
  def validate: String = null
}
object NULL extends Expression {
  override def toString = "null"
  def getType = NOTHING
  def validate: String = null
}
case class RValue(val v: String, val itype: Type) extends Expression {
  override def toString = v

  def getType = itype
  def validate: String = null
}
object NumberValue {
  def apply(x: String): RValue = {
    if (x.contains(".")) RValue(x, DOUBLE) else {
      val value = x.toLong
      if (value <= Byte.MaxValue) RValue(x, BYTE)
      else if (value <= Short.MaxValue) RValue(x, SHORT)
      else if (value <= Int.MaxValue) RValue(x, INT)
      else RValue(x + "L", LONG)
    }
  }
}
