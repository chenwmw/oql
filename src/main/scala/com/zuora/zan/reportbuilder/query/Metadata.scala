package com.zuora.zan.reportbuilder.query

trait Metadata {
  def getType(key: String): Type
}
