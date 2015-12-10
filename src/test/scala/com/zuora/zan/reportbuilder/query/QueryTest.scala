package com.zuora.zan.reportbuilder.query

import scala.Stream
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import java.util.Date

@RunWith(classOf[JUnitRunner])
class QueryTest extends QuerySpec {
  // NOTE: 'persons' is a table containing Person records. See QuerySpec for details.

  "For 'or', John, William and Tod" should "exist in the result" in {
    select from persons where "age > 35 or height > 1.80" map { _.name } shouldBe List("John", "William", "Tod")
  }
  "2nd For 'or', John, William and Tod" should "exist in the result" in {
    database execute "select name, age, height from persons where age > 35 or height > 1.80" foreach println
  }
  "For 'and', only John" should "exist in the result" in {
    select from persons where "age < 30 and height > 1.80" map { _.name } shouldBe List("John")
  }
  "For 'not', married persons" should "be Mary, William and Tod" in {
    select from persons where "not single" map { _.name } shouldBe List("Mary", "William", "Tod")
  }
  "For '=', only William " should "be born on Apr 27, 1977" in {
    select from persons where "birthdate = " + toDate("Apr 27, 1977").getTime map { _.name } shouldBe List("William")
  }
  "For '!=', none of John, Mary, Stella or Tod" should "be born on Apr 27, 1977" in {
    select from persons where "birthdate != " + toDate("Apr 27, 1977").getTime map { _.name } shouldBe List("John", "Mary", "Stella", "Tod")
  }
  "For '<>', none of John, Mary, Stella or Tod" should "be born on Apr 27, 1977" in {
    select from persons where "birthdate <> " + toDate("Apr 27, 1977").getTime map { _.name } shouldBe List("John", "Mary", "Stella", "Tod")
  }
  "For '>', the person who is taller than 1.80" should "be John" in {
    select from persons where "height > 1.80" map {_.name} shouldBe List("John")
  }
  "For '>=', the person who is 34 or older than 34" should "be Mary, William and Tod" in {
    select from persons where "age >= 34" map {_.name} shouldBe List("Mary", "William", "Tod")
  }
  "For '<', the persons who are shorter than 1.70" should "be Mary and William" in {
    select from persons where "height < 1.70" map {_.name} shouldBe List("Mary", "William")
  }
  "For '<=', the persons who are 24 or younger than 24" should "be John and Stella" in {
    select from persons where "age <= 24" map {_.name} shouldBe List("John", "Stella")
  }
  "For 'is null', the person whose birthdate is missing" should "be Tod" in {
    select from persons where "birthdate is null" map {_.name} shouldBe List("Tod")
  }
  "For 'is not null', the persons whose birthdate are not missing" should "be John, Mary, Stella and William" in {
    select from persons where "birthdate is not null" map {_.name} shouldBe List("John", "Mary", "Stella", "William")
  }
  "For 'in', the persons who are either 23, 24 or 34" should "be John, Mary and Stella" in {
    select from persons where "age in (23, 24, 34)" map {_.name} shouldBe List("John", "Mary", "Stella")
  }
  "for not 'in', the persons who is neither 23, 24 or 34" should "be William and Tod" in {
    select from persons where "age not in (23, 24, 34)" map {_.name} shouldBe List("William", "Tod")
  }
  "For 'like', the persons whose name has two adjacent l" should "be Stella and William" in {
    select from persons where """ name like ".*ll.*" """ map {_.name} shouldBe List("Stella", "William")
  }
  "For not 'like', the persons whose name does not has two adjacent l" should "be John, Mary and Tod" in {
    select from persons where """ name not like ".*ll.*" """ map {_.name} shouldBe List("John", "Mary", "Tod")
  }
  "For boolean field, the person who is single and taller than 1.80" should "be John" in {
    select from persons where "single and height > 1.80" map {_.name} shouldBe List("John")
  }
  "For negative number, the person whose age is -10" should "not exist at all" in {
    select from persons where "age = -1" map {_.name} shouldBe Nil
  }
  "For right ident, the person whose work is equal to his age" should "be Stella" in {
    select from persons where "age = work" map {_.name} shouldBe List("Stella")
  }
  "The person who is one year older than 23 and not single" should "be Mary, William and Tod" in {
    select from persons where "23 < age - 1 and not single" map {_.name} shouldBe List("Mary", "William", "Tod")
  }
  "The persons whose age is between 30 and 40" should "be Mary and William" in {
    select from persons where "age between 30 and 40" map {_.name} shouldBe List("Mary", "William")
  }
  "The persons whose age is not between 30 and 40" should "be John, Stella and Tod" in {
    select from persons where "age not between 30 and 40" map {_.name} shouldBe List("John", "Stella", "Tod")
  }
  "The persons whose last name is Chen " should "include John and William" in {
    select from persons where """ email.last = "Chen" """ map {_.name} shouldBe List("John", "William")
  }
} 