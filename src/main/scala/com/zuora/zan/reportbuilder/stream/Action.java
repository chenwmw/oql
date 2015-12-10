package com.zuora.zan.reportbuilder.stream;

public interface Action<T> {
	void apply(T o);
}
