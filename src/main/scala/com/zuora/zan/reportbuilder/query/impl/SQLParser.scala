package com.zuora.zan.reportbuilder.query.impl

import com.zuora.zan.reportbuilder.query._

class SQLParser(val meta: Metadata, val pmeta: Metadata) extends ExpressionParser(meta, pmeta) {
  protected lazy val sql: Parser[SQLExpression] = ("select" ~> fieldList) ~ ( "from" ~> tableName) ~ ("where" ~> expression) ^^ {case fl ~ tb ~ exp => SQLExpression(fl, tb, exp) }
  protected lazy val fieldList: Parser[List[String]] = rep1sep(field, ",")
  protected lazy val field:Parser[String] = ident
  protected lazy val tableName:Parser[String] = ident
}
