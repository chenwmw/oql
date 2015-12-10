package com.zuora.zan.reportbuilder.orm.impl;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.zuora.zan.reportbuilder.query.Query;

public class ResultSetHandler implements InvocationHandler {
	private ResultSet rs;
	private Query<Row> query;

	public <T> ResultSetHandler(ResultSet rs, Query<Row> query) {
		this.rs = rs;
		this.query = query;
	}

	@Override
	public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
		if (query != null) {
			if ("iterator".equals(method.getName())) {
				return query.execute(rs).iterator();
			} else if ("all".equals(method.getName())) {
				List<Row> filteredRows = new ArrayList<Row>();
				for(Row row:query.execute(rs)) {
					filteredRows.add(row);
				}
				return filteredRows;
			} else if ("one".equals(method.getName())) {
				Iterator<Row> iterator = query.execute(rs).iterator();
				return iterator.hasNext() ? iterator.next() : null;
			} else if ("getExecutionInfo".equals(method.getName())) {
				return rs.getExecutionInfo();
			}
		}
		return method.invoke(rs, args);
	}
}
