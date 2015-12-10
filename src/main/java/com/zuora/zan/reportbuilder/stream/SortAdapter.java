package com.zuora.zan.reportbuilder.stream;

import java.util.Comparator;

@SuppressWarnings("rawtypes")
public interface SortAdapter<T> {
	Comparator getComparator(String key);

	Object getValue(T o, String key);
}
