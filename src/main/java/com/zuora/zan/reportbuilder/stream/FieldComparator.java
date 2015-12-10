package com.zuora.zan.reportbuilder.stream;

import java.util.Comparator;

class FieldComparator<T> implements Comparator<T> {
	private OrderBy[] orderBys;
	private SortAdapter<T> adapter;

	public FieldComparator(SortAdapter<T> adapter, OrderBy[] orderBys) {
		this.orderBys = orderBys;
		this.adapter = adapter;
	}
	
	@SuppressWarnings("unchecked")
	private int compareFieldValue(Object v1, Object v2, String field) {
		return v1 == null ? (v2 == null ? 0 : -1) : (v2 == null ? 1 : adapter.getComparator(field).compare(v1, v2));
	}

	private int compareNonnull(T e1, T e2) {
		for (OrderBy orderBy : orderBys) {
			String field = orderBy.getField();
			Object v1 = adapter.getValue(e1, field);
			Object v2 = adapter.getValue(e2, field);
			int compare = compareFieldValue(v1, v2, field);
			if (compare != 0)
				return orderBy.isAscend() ? compare : -compare;
		}
		return 0;
	}

	@Override
	public int compare(T e1, T e2) {
		return e1 == null ? (e2 == null ? 0 : -1) : (e2 == null ? 1 : compareNonnull(e1, e2));
	}

}
