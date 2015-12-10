package com.zuora.zan.reportbuilder.query

trait Introspector {
  def get(key: String): Any
}