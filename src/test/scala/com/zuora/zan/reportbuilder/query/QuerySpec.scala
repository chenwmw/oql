package com.zuora.zan.reportbuilder.query

import org.scalatest.Matchers
import org.scalatest.FlatSpec
import java.util.Date
import java.text.DateFormat
import java.util.Locale
case class Email(
    val first: String, 
    val last: String, 
    val domain: String)
case class Person(
    val name: String, 
    val single: Boolean, 
    val age: Int, 
    val birthdate: Date, 
    val height: Double,
    val work: Int,
    val email: Email)

class QuerySpec extends FlatSpec with Matchers {
  protected def toDate(str:String): Date = DateFormat.getDateInstance(DateFormat.MEDIUM, Locale.US).parse(str)
  def persons: Stream[Person] = Stream(
        Person("John", true, 23, toDate("Apr 5, 1992"), 1.82, 1, Email("John", "Chen", "zuora.com")),
        Person("Mary", false, 34, toDate("Dec 10, 1981"), 1.62, 2, Email("Mary", "Wang", "zuora.com")),
        Person("Stella", true, 24, toDate("Jul 12, 1991"), 1.70, 24, Email("Stella", "Liu", "zuora.com")),
        Person("William", false, 38, toDate("Apr 27, 1977"), 1.65, 3, Email("William", "Chen", "zuora.com")),
        Person("Tod", false, 49, null, 1.78, 4, Email("Tod", "Fu", "zuora.com")))
  def select:Query[Person] = Select(classOf[Person])
  def database:SQLQuery[Person] = Database(classOf[Person], persons)
}

