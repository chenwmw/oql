package com.zuora.zan.reportbuilder.stream;

public interface Predicate<T> {
	boolean apply(T o);
}
