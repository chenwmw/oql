package com.zuora.zan.reportbuilder.orm.impl;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Map;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.mapping.Result;

@SuppressWarnings("rawtypes")
public class AccessorHandler<T> implements InvocationHandler {
	private T accessor;
	private Map<String, MethodFilter> filters;

	public AccessorHandler(T accessor, Map<String, MethodFilter> filters) {
		this.accessor = accessor;
		this.filters = filters;
	}

	private ResultSet getResultSet(Result result) {
		try {
			Field rsField = Result.class.getDeclaredField("rs");
			rsField.setAccessible(true);
			return (ResultSet) rsField.get(result);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	private void setResultSet(Result result, ResultSet rs) {
		try {
			Field rsField = Result.class.getDeclaredField("rs");
			rsField.setAccessible(true);
			rsField.set(result, rs);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
		Class<?> returnType = method.getReturnType();
		MethodFilter methodFilter = filters.get(method.toString());
		if (Iterable.class.isAssignableFrom(returnType) && methodFilter != null) {
			Result result;
			if (methodFilter.getSource() == null)
				result = (Result) method.invoke(accessor, args);
			else {
				Object[] srcArgs = methodFilter.prepareSrcArgs(args);
				result = (Result) methodFilter.getSource().invoke(accessor, srcArgs);
			}
			if (Result.class.isAssignableFrom(returnType)) {
				ResultSet rs = getResultSet(result);
				rs = (ResultSet) Proxy.newProxyInstance(getClass().getClassLoader(), new Class[] { ResultSet.class },
						new ResultSetHandler(rs, methodFilter.bindRow(args)));
				setResultSet(result, rs);
				return result;
			} else {
				return methodFilter.bindBean(args).execute(result);
			}
		}
		return method.invoke(accessor, args);
	}
}