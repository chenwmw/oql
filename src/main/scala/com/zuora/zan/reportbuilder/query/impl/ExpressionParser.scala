package com.zuora.zan.reportbuilder.query.impl
import com.zuora.zan.reportbuilder.query._

import scala.Ordering
import scala.util.parsing.combinator.JavaTokenParsers
import java.util.Date
import scala.collection.JavaConversions._

class ExpressionParser(val metadata: Metadata, val paramMeta: Metadata) extends JavaTokenParsers {
  protected var IDs: Set[List[String]] = Set()
  protected var PARAMs: Set[String] = Set()
  protected lazy val expression: Parser[Expression] = orExpression
  protected lazy val orExpression: Parser[Expression] = andExpression * ("or" ^^^ { (e1: Expression, e2: Expression) => Or(e1, e2) })
  protected lazy val andExpression: Parser[Expression] = comparisonExpression * ("and" ^^^ { (e1: Expression, e2: Expression) => And(e1, e2) })
  protected lazy val comparisonExpression: Parser[Expression] =
    (
      "not" ~> termExpression ^^ { Not }
      | termExpression ~ ("=" ~> termExpression) ^^ { case e1 ~ e2 => EqualTo(e1, e2) }
      | termExpression ~ ("<" ~> termExpression) ^^ { case e1 ~ e2 => LessThan(e1, e2) }
      | termExpression ~ ("<=" ~> termExpression) ^^ { case e1 ~ e2 => LessThanOrEqual(e1, e2) }
      | termExpression ~ (">" ~> termExpression) ^^ { case e1 ~ e2 => GreaterThan(e1, e2) }
      | termExpression ~ (">=" ~> termExpression) ^^ { case e1 ~ e2 => GreaterThanOrEqual(e1, e2) }
      | termExpression ~ ("!=" ~> termExpression) ^^ { case e1 ~ e2 => NotEqualTo(e1, e2) }
      | termExpression ~ ("<>" ~> termExpression) ^^ { case e1 ~ e2 => NotEqualTo(e1, e2) }
      | termExpression ~ "not".? ~ ("between" ~> termExpression) ~ ("and" ~> termExpression) ^^ {
        case e ~ not ~ el ~ eu =>
          val betweenExpr: Expression = And(GreaterThanOrEqual(e, el), LessThanOrEqual(e, eu))
          not.fold(betweenExpr)(f => Not(betweenExpr))
      }
      | termExpression ~ ("like" ~> termExpression) ^^ { case e1 ~ e2 => new Like(e1, e2) }
      | termExpression ~ ("not" ~ "like" ~> termExpression) ^^ { case e1 ~ e2 => NotLike(e1, e2) }
      | termExpression ~ ("in" ~ "(" ~> rep1sep(termExpression, ",")) <~ ")" ^^ {
        case e1 ~ e2 => new In(e1, e2)
      }
      | termExpression ~ ("not" ~ "in" ~ "(" ~> rep1sep(termExpression, ",")) <~ ")" ^^ {
        case e1 ~ e2 => NotIn(e1, e2)
      }
      | termExpression <~ "is" ~ "null" ^^ { e => new IsNull(e) }
      | termExpression <~ "is" ~ "not" ~ "null" ^^ { e => NotIsNull(e) }
      | termExpression)
  protected lazy val termExpression: Parser[Expression] =
    productExpression *
      ("+" ^^^ { (e1: Expression, e2: Expression) => Add(e1, e2) }
        | "-" ^^^ { (e1: Expression, e2: Expression) => Subtract(e1, e2) })

  protected lazy val productExpression: Parser[Expression] =
    baseExpression *
      ("*" ^^^ { (e1: Expression, e2: Expression) => Multiply(e1, e2) }
        | "/" ^^^ { (e1: Expression, e2: Expression) => Divide(e1, e2) }
        | "%" ^^^ { (e1: Expression, e2: Expression) => Remainder(e1, e2) })

  protected lazy val sign: Parser[String] =
    "+" | "-"

  protected lazy val baseExpression: Parser[Expression] = primary

  protected lazy val signedPrimary: Parser[Expression] =
    sign ~ primary ^^ { case s ~ e => if (s == "-") UnaryMinus(e) else e }

  protected lazy val primary: Parser[Expression] =
    (right_value
      | "(" ~> expression <~ ")"
      | rep1sep(ident, ".") ^^ { x => IDs = IDs+x; IdentValue(x, metadata.getType(x mkString ".")) }
      | prepared_param
      | signedPrimary)

  protected lazy val prepared_param: Parser[Expression] = ":" ~> ident ^^ { x => PARAMs = PARAMs + x; ParamValue(x, paramMeta.getType(x)) }
  protected lazy val right_value: Parser[Expression] = decimal | literal | nullvalue | booleanValue
  protected lazy val nullvalue: Parser[Expression] = "null" ^^ { x => NULL }
  protected lazy val decimal: Parser[Expression] = decimalNumber ^^ { x => NumberValue(x) }
  protected lazy val literal: Parser[Expression] = stringLiteral ^^ { x => RValue(x.replace("\\\\", "\\"), STRING) }
  protected lazy val booleanValue: Parser[Expression] = ("true" | "false") ^^ { x => RValue(x, BOOLEAN) }
}