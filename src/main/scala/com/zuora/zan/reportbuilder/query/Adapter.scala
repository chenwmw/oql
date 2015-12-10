package com.zuora.zan.reportbuilder.query

import java.util.Comparator

trait Adapter[K] {
  def get(obj: K, key: String): Any
}

