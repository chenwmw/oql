package com.zuora.zan.reportbuilder.query.impl
import java.lang.reflect._
import com.zuora.zan.reportbuilder.query._
import com.zuora.zan.reportbuilder.compiler._
import scala.collection.JavaConversions._
import java.util.{Map => JMap}
import java.lang.{Iterable => JIterable}
import java.util.{Iterator => JIterator}

case class PreparedParam(val metadata: Metadata, val names: Set[String])

object ClassIndex {
  var index: Long = -1
  def nextIndex: Long = { index = index + 1; index }
}

class EvaluatorLoader(val metadata: Metadata, val ids: Set[List[String]], val param: PreparedParam, val expression: String) {
  val simpleName = "FilterIterable" + ClassIndex.nextIndex
  val paramClass = classOf[JMap[String,Object]]
  val iterableClass = classOf[JIterable[_]]
  val filterClass = ParserClassLoader.compile("com.zuora.zan.reportbuilder.query.impl." + simpleName,
    generateCode(metadata, ids, param, expression))

  def load[K](iterable: Iterable[JMap[String,Object]], params: JMap[String, Object]): Iterable[JMap[String,Object]] = load[K](asJavaIterable(iterable), params)

  def load[K](iterable: JIterable[JMap[String, Object]], params: JMap[String, Object]): JIterable[JMap[String, Object]] =
  filterClass.getConstructor(iterableClass, paramClass).newInstance(iterable, params).asInstanceOf[JIterable[JMap[String, Object]]]
  
  private def createCons(param: PreparedParam) = if (param != null) s"""
  public $simpleName(Iterable<Map<String,Object>> iterable, Map<String,Object> params) {
    this.iterator = iterable.iterator();
    ${createInitFields(param)}
  }"""
  else ""

  private def createVarDecl(key: String, name: String, objName: String, metadata: Metadata) =
    s"""${metadata.getType(key).getJavaType} $name = (${metadata.getType(key).getWrapperType})$objName.get("$key");"""

  private def createVars(ids: Set[List[String]]) =
    { for { id <- ids; line = createVarDecl(id mkString ".", id mkString "_", "context", metadata) } yield line } mkString "\n\t"

  private def createFieldDecl(key: String, name: String, metadata: Metadata) =
    s"""private ${metadata.getType(key).getJavaType} $name;"""

  private def createParFields(param: PreparedParam) =
    if (param != null && !param.names.isEmpty)
      { for { name <- param.names; line = createFieldDecl(name, name, param.metadata) } yield line } mkString "\n\t"
    else ""

  private def createInitField(key: String, name: String, objName: String, metadata: Metadata) =
    s"""this.$name = (${metadata.getType(key).getWrapperType})$objName.get("$key");"""

  private def createInitFields(param: PreparedParam) =
    if (param != null && !param.names.isEmpty)
      { for { name <- param.names; line = createInitField(name, name, "params", param.metadata) } yield line } mkString "\n\t"
    else ""

  private def generateCode(metadata: Metadata, ids: Set[List[String]], param: PreparedParam, expression: String) = new java.lang.StringBuilder append s"""
package com.zuora.zan.reportbuilder.query.impl;

import java.util.Arrays;
import java.util.regex.Pattern;
import java.util.Map;
import java.util.Date;
import java.util.Iterator;
import java.lang.Iterable;

public class $simpleName implements Iterator<Map<String,Object>>, Iterable<Map<String, Object>> {
  private Map<String,Object> current;
  private Iterator<Map<String,Object>> iterator;
  ${createParFields(param)}
    
  ${createCons(param)}
    
  private int compareString(String l, String r) {
    return l == null ? (r == null ? 0 : -1) : l.compareTo(r);
  }
    
  private int compareDate(Date l, Date r) {
    return l == null ? (r == null ? 0 : -1) : l.compareTo(r);
  }
    
  private int compareDate(long l, Date r) {
    return r == null ? 1 : (l < r.getTime() ? -1 : (l == r.getTime() ? 0 : 1));
  }
    
  private int compareDate(Date l, long r) {
    return l == null ? -1 : (l.getTime() < r ? -1 : (l.getTime() == r ? 0 : 1));
  }
    
  private boolean like(String l, String r) {
    return (l == null || r == null) ? false : Pattern.compile(r).matcher(l).find(0);
  }
    
  private boolean evaluate(Map<String,Object> context) {
    ${createVars(ids)}
    return $expression;
  }
  
  private Map<String,Object> findNext() {
    while (iterator.hasNext()) {
      Map<String,Object> o = iterator.next();
      if (evaluate(o))
        return o;
    }
    return null;
  }

  @Override
  public boolean hasNext() {
    current = findNext();
    return current != null;
  }

  @Override
  public Map<String,Object> next() {
    return current;
  }

  @Override
  public void remove() {
    iterator.remove();
  }

  @Override
  public Iterator<Map<String, Object>> iterator() {
    return this;
  }
}
"""
}
