package com.zuora.zan.reportbuilder.query

import scala.collection.JavaConversions._

import com.zuora.zan.reportbuilder.query.impl._
import java.lang.{ Iterable => JIterable }
import java.util.{ Map => JMap }
import java.util.{ HashMap => JHashMap }
import java.util.{ Iterator => JIterator }
import java.util.{ List => JList }
import com.zuora.zan.reportbuilder.stream.{ Stream => JStream }
import com.zuora.zan.reportbuilder.stream._

case class WrappingMap(val o: Introspector) extends JHashMap[String, Object] {
  override def get(key: Object): Object = o.get(key.asInstanceOf[String]).asInstanceOf[Object]
}

case class AdapterMap[K](val obj: K, val adapter: Adapter[K]) extends JHashMap[String, Object] {
  override def get(key: Object): Object = adapter.get(obj, key.asInstanceOf[String]).asInstanceOf[Object]
}
class ProjectionMap extends JHashMap[String, Object] {
  def withValues(names:List[String], value:JMap[String, Object]):ProjectionMap = {
    for(name <- names) {
      if(value(name) != null)
        put(name, value(name))
    }
    this
  }
}
object Database {
  def apply[K](meta:Metadata, data:Iterable[K]) : SQLQuery[K] = new SQLQuery[K](meta).from(data)
  def apply[K](eClass: Class[K], data:Iterable[K]) : SQLQuery[K] = apply(BeanAdapter(eClass), data)
}
class SQLQuery[K](val m: Metadata, val p: Metadata) extends SQLParser(m, p) {
  var fields: List[String] = null
  var table: Iterable[K] = null
  var loader: EvaluatorLoader = null
  var params: JMap[String, Object] = null
  def this(meta: Metadata) = this(meta, null)

  lazy val predicate: K => JMap[String, Object] = x =>
    x match {
      case i: Introspector => WrappingMap(i)
      case _ => meta match {
        case adapter: Adapter[K] => AdapterMap(x, adapter)
      }
    }
  lazy val projection: JMap[String, Object] => JMap[String, Object] = x => new ProjectionMap().withValues(fields, x)
  def bind(params: JMap[String, Object]): SQLQuery[K] = {
    if (params != null)
      this.params = params
    this
  }
  def from(table: Iterable[K]): SQLQuery[K] = { this.table = table; this }
  def execute(querySql:String):Iterable[JMap[String, Object]] = {
    val parse = parseAll(sql, querySql);
    if (parse.successful) {
      val sqlExp = parse.get
      val msg = sqlExp.filterExp.validate
      if (msg == null) loader = new EvaluatorLoader(meta, IDs, new PreparedParam(pmeta, PARAMs), sqlExp.filterExp.toString)
      else throw new IllegalArgumentException("Parsing OQL failed:" + msg)
      fields = sqlExp.fieldList
    } else throw new IllegalArgumentException("Parsing OQL failed:" + parse)
    loader.load(table.map { predicate }, params).map { projection }    
  }
}
class Query[K](val meta: Metadata, val pmeta: Metadata) extends ExpressionParser(meta, pmeta) {
  val predicate: K => JMap[String, Object] = x =>
    x match {
      case i: Introspector => WrappingMap(i)
      case _ => meta match {
        case adapter: Adapter[K] => AdapterMap(x, adapter)
      }
    }
  val projection: JMap[String, Object] => K = x => x match {
    case wm: WrappingMap => wm.o.asInstanceOf[K]
    case am: AdapterMap[K] => am.obj
  }

  var loader: EvaluatorLoader = null
  var table: Iterable[K] = null
  var params: JMap[String, Object] = null

  def this(meta: Metadata) = this(meta, null)

  def filter(clause: String): Query[K] = {
    val parse = parseAll(expression, clause);
    if (parse.successful) {
      val exp = parse.get
      val msg = exp.validate
      if (msg == null) loader = new EvaluatorLoader(meta, IDs, new PreparedParam(pmeta, PARAMs), exp.toString)
      else throw new IllegalArgumentException("Parsing OQL failed:" + msg)
    } else throw new IllegalArgumentException("Parsing OQL failed:" + parse)
    this
  }

  def execute(iterable: JIterable[K]): JIterable[K] = { val sIter: Iterable[K] = iterable; asJavaIterable(execute(sIter)) }

  def execute(iterable: Iterable[K]): Iterable[K] = loader.load(iterable.map { predicate }, params).map { projection }

  def bind(params: JMap[String, Object]): Query[K] = {
    if (params != null)
      this.params = params
    this
  }

  def from(table: Iterable[K]): Query[K] = { this.table = table; this }
  def from(table: JIterable[K]): Query[K] = { this.table = table; this }
  def where(clause: String): Iterable[K] = filter(clause).execute(table)
  def withClause(clause: String): JList[K] = where(clause).toList
}
object Select {
  def apply[K](eClass: Class[K]): Query[K] = new Query[K](BeanAdapter(eClass))
}
object Query {
  def apply[K](eClass: Class[K]): Query[K] = new Query[K](BeanAdapter(eClass))
  def build[K](meta: Metadata): Query[K] = new Query[K](meta)
  def build[K](eClass: Class[K]): Query[K] = new Query[K](BeanAdapter(eClass))
}